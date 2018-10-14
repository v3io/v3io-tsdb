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
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/appender"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/querier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	pathUtil "path"
	"time"
)

type V3ioAdapter struct {
	startTimeMargin int64
	logger          logger.Logger
	container       *v3io.Container
	MetricsCache    *appender.MetricsCache
	cfg             *config.V3ioConfig
	partitionMngr   *partmgr.PartitionManager
}

func CreateTSDB(v3iocfg *config.V3ioConfig, schema *config.Schema) error {

	lgr, _ := utils.NewLogger(v3iocfg.LogLevel)
	container, err := utils.CreateContainer(
		lgr, v3iocfg.WebApiEndpoint, v3iocfg.Container, v3iocfg.Username, v3iocfg.Password, v3iocfg.Workers)
	if err != nil {
		return errors.Wrap(err, "Failed to create a data container.")
	}

	data, err := json.Marshal(schema)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal the TSDB schema file.")
	}

	path := pathUtil.Join(v3iocfg.TablePath, config.SchemaConfigFileName)
	// Check whether the config file already exists, and abort if it does
	_, err = container.Sync.GetObject(&v3io.GetObjectInput{Path: path})
	if err == nil {
		return fmt.Errorf("A TSDB table already exists at path '" + v3iocfg.TablePath + "'.")
	}

	err = container.Sync.PutObject(&v3io.PutObjectInput{Path: path, Body: data})
	if err != nil {
		return errors.Wrapf(err, "Failed to create a TSDB schema at path '%s'.",
			pathUtil.Join(v3iocfg.WebApiEndpoint, v3iocfg.Container, path))
	}
	return err
}

// Create a new TSDB adapter, similar to Prometheus TSDB adapter but with a few
// extensions. The Prometheus compliant adapter is found under /promtsdb.
func NewV3ioAdapter(cfg *config.V3ioConfig, container *v3io.Container, logger logger.Logger) (*V3ioAdapter, error) {

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

	if container != nil {
		newV3ioAdapter.container = container
	} else {
		newV3ioAdapter.container, err = utils.CreateContainer(newV3ioAdapter.logger,
			cfg.WebApiEndpoint, cfg.Container, cfg.Username, cfg.Password, cfg.Workers)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create V3IO data container")
		}
	}

	err = newV3ioAdapter.connect()

	return &newV3ioAdapter, err
}

func (a *V3ioAdapter) GetSchema() *config.Schema {
	return a.partitionMngr.GetConfig()
}

func (a *V3ioAdapter) GetLogger(child string) logger.Logger {
	return a.logger.GetChild(child)
}

func (a *V3ioAdapter) GetContainer() (*v3io.Container, string) {
	return a.container, a.cfg.TablePath
}

func (a *V3ioAdapter) connect() error {

	fullpath := pathUtil.Join(a.cfg.WebApiEndpoint, a.cfg.Container, a.cfg.TablePath)
	resp, err := a.container.Sync.GetObject(&v3io.GetObjectInput{Path: pathUtil.Join(a.cfg.TablePath, config.SchemaConfigFileName)})
	if err != nil {
		if utils.IsNotExistsError(err) {
			return errors.Errorf("No TSDB schema file found at '%s'.", fullpath)
		} else {
			return errors.Wrapf(err, "Failed to read a TSDB schema from '%s'.", fullpath)
		}

	}

	tableSchema := config.Schema{}
	err = json.Unmarshal(resp.Body(), &tableSchema)
	if err != nil {
		return errors.Wrapf(err, "Failed to unmarshal the TSDB schema at '%s'.", fullpath)
	}

	if tableSchema.TableSchemaInfo.Version != schema.Version {
		return errors.Errorf("Table Schema version mismatch - existing table schema version is %d while the tsdb library version is %d! Make sure to create the table with same library version",
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

	a.logger.Info("Starting the V3IO TSDB client for the TSDB instance at '%s'", fullpath)
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
	startTime := int64(time.Now().Unix() * 1000)
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

func (a *V3ioAdapter) DeleteDB(deleteAll bool, ignoreErrors bool, fromTime int64, toTime int64) error {
	if deleteAll {
		// Ignore time boundaries
		fromTime = 0
		toTime = time.Now().Unix() * 1000
	}

	partitions := a.partitionMngr.PartsForRange(fromTime, toTime)
	for _, part := range partitions {
		a.logger.Info("Delete partition '%s'.", part.GetTablePath())
		err := utils.DeleteTable(a.logger, a.container, part.GetTablePath(), "", a.cfg.QryWorkers)
		if err != nil && !ignoreErrors {
			return errors.Wrapf(err, "Failed to delete partition '%s'.", part.GetTablePath())
		}
		// Delete the Directory object
		err = a.container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: part.GetTablePath()})
		if err != nil && !ignoreErrors {
			return errors.Wrapf(err, "Failed to delete partition object '%s'.", part.GetTablePath())
		}
	}
	a.partitionMngr.DeletePartitionsFromSchema(partitions)

	if len(a.partitionMngr.GetPartitionsPaths()) == 0 {
		path := a.cfg.TablePath + "/names/"
		a.logger.Info("Delete metric names at path '%s'.", path)
		err := utils.DeleteTable(a.logger, a.container, path, "", a.cfg.QryWorkers)
		if err != nil && !ignoreErrors {
			return errors.Wrap(err, "Failed to delete the metric-names table.")
		}
		// Delete the Directory object
		err = a.container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: path})
		if err != nil && !ignoreErrors {
			if !utils.IsNotExistsError(err) {
				return errors.Wrapf(err, "Failed to delete table object '%s'.", path)
			}
		}
	}
	if deleteAll {
		schemaPath := pathUtil.Join(a.cfg.TablePath, config.SchemaConfigFileName)
		a.logger.Info("Delete the TSDB configuration at '%s'.", schemaPath)
		err := a.container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: schemaPath})
		if err != nil && !ignoreErrors {
			return errors.New("The configuration at '" + schemaPath + "' cannot be deleted or doesn't exist.")
		}
		// Delete the Directory object
		path := a.cfg.TablePath + "/"
		err = a.container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: path})
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
func (a v3ioAppender) Add(lset utils.Labels, t int64, v float64) (uint64, error) {
	return a.metricsCache.Add(lset, t, v)
}

// Faster Add using refID obtained from Add (avoid some hash/lookup overhead)
func (a v3ioAppender) AddFast(lset utils.Labels, ref uint64, t int64, v float64) error {
	return a.metricsCache.AddFast(ref, t, v)
}

// Wait for completion of all updates
func (a v3ioAppender) WaitForCompletion(timeout time.Duration) (int, error) {
	return a.metricsCache.WaitForCompletion(timeout)
}

// In V3IO, all operations are committed (no client cache)
func (a v3ioAppender) Commit() error   { return nil }
func (a v3ioAppender) Rollback() error { return nil }

// The Appender interface provides batched appends against a storage.
type Appender interface {
	Add(l utils.Labels, t int64, v float64) (uint64, error)
	AddFast(l utils.Labels, ref uint64, t int64, v float64) error
	WaitForCompletion(timeout time.Duration) (int, error)
	Commit() error
	Rollback() error
}
