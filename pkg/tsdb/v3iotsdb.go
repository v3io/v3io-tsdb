/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package tsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	pathUtil "path"
	"path/filepath"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-go/pkg/dataplane/http"
	"github.com/v3io/v3io-tsdb/pkg/appender"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/querier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const defaultHTTPTimeout = 30 * time.Second

type V3ioAdapter struct {
	startTimeMargin int64
	logger          logger.Logger
	container       v3io.Container
	HTTPTimeout     time.Duration
	MetricsCache    *appender.MetricsCache
	cfg             *config.V3ioConfig
	partitionMngr   *partmgr.PartitionManager
}

func CreateTSDB(cfg *config.V3ioConfig, schema *config.Schema) error {

	lgr, _ := utils.NewLogger(cfg.LogLevel)
	httpTimeout := parseHTTPTimeout(cfg, lgr)
	container, err := utils.CreateContainer(lgr, cfg, httpTimeout)
	if err != nil {
		return errors.Wrap(err, "Failed to create a data container.")
	}

	data, err := json.Marshal(schema)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal the TSDB schema file.")
	}

	dataPlaneInput := v3io.DataPlaneInput{Timeout: httpTimeout}

	path := pathUtil.Join(cfg.TablePath, config.SchemaConfigFileName)
	// Check whether the config file already exists, and abort if it does
	_, err = container.GetObjectSync(&v3io.GetObjectInput{Path: path, DataPlaneInput: dataPlaneInput})
	if err == nil {
		return fmt.Errorf("A TSDB table already exists at path '" + cfg.TablePath + "'.")
	}

	err = container.PutObjectSync(&v3io.PutObjectInput{Path: path, Body: data, DataPlaneInput: dataPlaneInput})
	if err != nil {
		return errors.Wrapf(err, "Failed to create a TSDB schema at path '%s/%s/%s'.", cfg.WebAPIEndpoint, cfg.Container, path)
	}
	return err
}

func parseHTTPTimeout(cfg *config.V3ioConfig, logger logger.Logger) time.Duration {
	if cfg.HTTPTimeout == "" {
		return defaultHTTPTimeout
	}
	timeout, err := time.ParseDuration(cfg.HTTPTimeout)
	if err != nil {
		logger.Warn("Failed to parse httpTimeout '%s'. Defaulting to %d millis.", cfg.HTTPTimeout, defaultHTTPTimeout/time.Millisecond)
		return defaultHTTPTimeout
	}
	return timeout
}

// Create a new TSDB adapter, similar to Prometheus TSDB adapter but with a few
// extensions. The Prometheus compliant adapter is found under /promtsdb.
func NewV3ioAdapter(cfg *config.V3ioConfig, container v3io.Container, logger logger.Logger) (*V3ioAdapter, error) {

	var err error
	newV3ioAdapter := V3ioAdapter{}
	newV3ioAdapter.cfg = cfg
	if logger != nil {
		newV3ioAdapter.logger = logger
	} else {
		newV3ioAdapter.logger, err = utils.NewLogger(cfg.LogLevel)
		if err != nil {
			return nil, err
		}
	}

	newV3ioAdapter.HTTPTimeout = parseHTTPTimeout(cfg, logger)

	if container != nil {
		newV3ioAdapter.container = container
	} else {
		newV3ioAdapter.container, err = utils.CreateContainer(newV3ioAdapter.logger, cfg, newV3ioAdapter.HTTPTimeout)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create V3IO data container")
		}
	}

	err = newV3ioAdapter.connect()

	return &newV3ioAdapter, err
}

func NewContainer(v3ioURL string, numWorkers int, accessKey string, username string, password string, containerName string, logger logger.Logger) (v3io.Container, error) {
	ctx, err := v3iohttp.NewContext(logger, v3iohttp.NewDefaultClient(), &v3io.NewContextInput{NumWorkers: numWorkers})
	if err != nil {
		return nil, err
	}

	session, err := ctx.NewSession(&v3io.NewSessionInput{URL: v3ioURL, Username: username, Password: password, AccessKey: accessKey})
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create session.")
	}

	container, err := session.NewContainer(&v3io.NewContainerInput{ContainerName: containerName})
	if err != nil {
		return nil, err
	}
	return container, nil
}

func (a *V3ioAdapter) GetSchema() *config.Schema {
	return a.partitionMngr.GetConfig()
}

func (a *V3ioAdapter) GetLogger(child string) logger.Logger {
	return a.logger.GetChild(child)
}

func (a *V3ioAdapter) GetContainer() (v3io.Container, string) {
	return a.container, a.cfg.TablePath
}

func (a *V3ioAdapter) connect() error {

	fullpath := fmt.Sprintf("%s/%s/%s", a.cfg.WebAPIEndpoint, a.cfg.Container, a.cfg.TablePath)
	resp, err := a.container.GetObjectSync(&v3io.GetObjectInput{Path: pathUtil.Join(a.cfg.TablePath, config.SchemaConfigFileName)})
	if err != nil {
		if utils.IsNotExistsError(err) {
			return errors.Errorf("No TSDB schema file found at '%s'.", fullpath)
		}
		return errors.Wrapf(err, "Failed to read a TSDB schema from '%s'.", fullpath)
	}

	tableSchema := config.Schema{}
	err = json.Unmarshal(resp.Body(), &tableSchema)
	if err != nil {
		return errors.Wrapf(err, "Failed to unmarshal the TSDB schema at '%s', got: %v .", fullpath, string(resp.Body()))
	}

	// in order to support backward compatibility we do not fail on version mismatch and only logging warning
	if a.cfg.LoadPartitionsFromSchemaAttr && tableSchema.TableSchemaInfo.Version != schema.Version {
		a.logger.Warn("Table Schema version mismatch - existing table schema version is %d while the tsdb library version is %d! Make sure to create the table with same library version",
			tableSchema.TableSchemaInfo.Version, schema.Version)
	}

	a.partitionMngr, err = partmgr.NewPartitionMngr(&tableSchema, a.container, a.cfg)
	if err != nil {
		return errors.Wrapf(err, "Failed to create a TSDB partition manager at '%s'.", fullpath)
	}
	err = a.partitionMngr.Init()
	if err != nil {
		return errors.Wrapf(err, "Failed to initialize the TSDB partition manager at: %s", fullpath)
	}

	a.logger.Debug("Running with the following TSDB configuration: %+v\n", a.cfg)

	return nil
}

func (a *V3ioAdapter) InitAppenderCache() error {
	if a.MetricsCache == nil {
		a.MetricsCache = appender.NewMetricsCache(a.container, a.logger, a.cfg, a.partitionMngr)
		return a.MetricsCache.Start()
	}

	return nil
}

// Create an appender interface, for writing performance
func (a *V3ioAdapter) Appender() (Appender, error) {
	err := a.InitAppenderCache()
	if err != nil {
		return nil, err
	}

	newAppender := v3ioAppender{metricsCache: a.MetricsCache}
	return newAppender, nil
}

func (a *V3ioAdapter) StartTime() (int64, error) {
	startTime := time.Now().Unix() * 1000
	return startTime - 1000*3600*24*1000, nil // TODO: from config or DB w default
}

func (a *V3ioAdapter) Close() error {
	return nil
}

// Create a Querier interface, used for time-series queries
func (a *V3ioAdapter) Querier(_ context.Context, mint, maxt int64) (*querier.V3ioQuerier, error) {
	if maxt < mint {
		return nil, errors.Errorf("End time '%d' is lower than start time '%d'.", maxt, mint)
	}
	return querier.NewV3ioQuerier(a.container, a.logger, mint, maxt, a.cfg, a.partitionMngr), nil
}

// Create a Querier interface, used for time-series queries
func (a *V3ioAdapter) QuerierV2() (*pquerier.V3ioQuerier, error) {
	return pquerier.NewV3ioQuerier(a.container, a.logger, a.cfg, a.partitionMngr), nil
}

func (a *V3ioAdapter) DeleteDB(deleteAll bool, ignoreErrors bool, fromTime int64, toTime int64) error {
	if deleteAll {
		// Ignore time boundaries
		fromTime = 0
		toTime = math.MaxInt64
	}

	partitions := a.partitionMngr.PartsForRange(fromTime, toTime, false)
	for _, part := range partitions {
		a.logger.Info("Deleting partition '%s'.", part.GetTablePath())
		err := utils.DeleteTable(a.logger, a.container, part.GetTablePath(), "", a.cfg.QryWorkers)
		if err != nil && !ignoreErrors {
			return errors.Wrapf(err, "Failed to delete partition '%s'.", part.GetTablePath())
		}
		// Delete the Directory object
		err = a.container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: part.GetTablePath()})
		if err != nil && !ignoreErrors {
			return errors.Wrapf(err, "Failed to delete partition object '%s'.", part.GetTablePath())
		}
	}
	err := a.partitionMngr.DeletePartitionsFromSchema(partitions)
	if err != nil {
		return err
	}

	if len(a.partitionMngr.GetPartitionsPaths()) == 0 {
		path := filepath.Join(a.cfg.TablePath, config.NamesDirectory) + "/" // Need a trailing slash
		a.logger.Info("Delete metric names at path '%s'.", path)
		err := utils.DeleteTable(a.logger, a.container, path, "", a.cfg.QryWorkers)
		if err != nil && !ignoreErrors {
			return errors.Wrap(err, "Failed to delete the metric-names table.")
		}
		// Delete the Directory object
		err = a.container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: path})
		if err != nil && !ignoreErrors {
			if !utils.IsNotExistsError(err) {
				return errors.Wrapf(err, "Failed to delete table object '%s'.", path)
			}
		}
	}
	if deleteAll {
		// Delete Schema file
		schemaPath := pathUtil.Join(a.cfg.TablePath, config.SchemaConfigFileName)
		a.logger.Info("Delete the TSDB configuration at '%s'.", schemaPath)
		err := a.container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: schemaPath})
		if err != nil && !ignoreErrors {
			return errors.New("The configuration at '" + schemaPath + "' cannot be deleted or doesn't exist.")
		}

		// Delete the Directory object
		path := a.cfg.TablePath + "/"
		err = a.container.DeleteObjectSync(&v3io.DeleteObjectInput{Path: path})
		if err != nil && !ignoreErrors {
			if !utils.IsNotExistsError(err) {
				return errors.Wrapf(err, "Failed to delete table object '%s'.", path)
			}
		}
	}

	return nil
}

// Return the number of items in a TSDB table
func (a *V3ioAdapter) CountMetrics(part string) (int, error) {
	count := 0
	paths := a.partitionMngr.GetPartitionsPaths()
	for _, path := range paths {
		input := v3io.GetItemsInput{Path: path, Filter: "", AttributeNames: []string{"__size"}}
		iter, err := utils.NewAsyncItemsCursor(a.container, &input, a.cfg.QryWorkers, []string{}, a.logger)
		if err != nil {
			return 0, err
		}

		for iter.Next() {
			count++
		}
		if iter.Err() != nil {
			return count, errors.Wrap(iter.Err(), "Failed on count iterator.")
		}
	}

	return count, nil
}

type v3ioAppender struct {
	metricsCache *appender.MetricsCache
}

// Add a t/v value to a metric item and return refID (for AddFast)
func (a v3ioAppender) Add(lset utils.Labels, t int64, v interface{}) (uint64, error) {
	return a.metricsCache.Add(lset, t, v)
}

// Faster Add using refID obtained from Add (avoid some hash/lookup overhead)
func (a v3ioAppender) AddFast(lset utils.Labels, ref uint64, t int64, v interface{}) error {
	return a.metricsCache.AddFast(ref, t, v)
}

// Wait for completion of all updates
func (a v3ioAppender) WaitForCompletion(timeout time.Duration) (int, error) {
	return a.metricsCache.WaitForCompletion(timeout)
}

func (a v3ioAppender) Close() {
	a.metricsCache.Close()
}

// In V3IO, all operations are committed (no client cache)
func (a v3ioAppender) Commit() error   { return nil }
func (a v3ioAppender) Rollback() error { return nil }

// The Appender interface provides batched appends against a storage.
type Appender interface {
	Add(l utils.Labels, t int64, v interface{}) (uint64, error)
	AddFast(l utils.Labels, ref uint64, t int64, v interface{}) error
	WaitForCompletion(timeout time.Duration) (int, error)
	Commit() error
	Rollback() error
	Close()
}
