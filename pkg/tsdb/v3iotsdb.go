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

	logger, _ := utils.NewLogger(v3iocfg.Verbose)
	container, err := utils.CreateContainer(
		logger, v3iocfg.V3ioUrl, v3iocfg.Container, v3iocfg.Username, v3iocfg.Password, v3iocfg.Workers)
	if err != nil {
		return errors.Wrap(err, "Failed to create data container")
	}

	data, err := json.Marshal(schema)
	if err != nil {
		return errors.Wrap(err, "Failed to Marshal schema file")
	}

	// check if the config file already exist, abort if it does
	_, err = container.Sync.GetObject(&v3io.GetObjectInput{Path: pathUtil.Join(v3iocfg.Path, config.SCHEMA_CONFIG)})
	if err == nil {
		return fmt.Errorf("TSDB already exist in path: " + v3iocfg.Path)
	}

	err = container.Sync.PutObject(&v3io.PutObjectInput{Path: pathUtil.Join(v3iocfg.Path, config.SCHEMA_CONFIG), Body: data})

	return err
}

// Create a new TSDB Adapter, similar to Prometheus TSDB Adapter with few extensions
// Prometheus compliant Adapter is under /promtsdb
func NewV3ioAdapter(cfg *config.V3ioConfig, container *v3io.Container, logger logger.Logger) (*V3ioAdapter, error) {

	var err error
	newV3ioAdapter := V3ioAdapter{}
	newV3ioAdapter.cfg = cfg
	if logger != nil {
		newV3ioAdapter.logger = logger
	} else {
		newV3ioAdapter.logger, err = utils.NewLogger(cfg.Verbose)
		if err != nil {
			return nil, err
		}
	}

	if container != nil {
		newV3ioAdapter.container = container
	} else {
		newV3ioAdapter.container, err = utils.CreateContainer(newV3ioAdapter.logger,
			cfg.V3ioUrl, cfg.Container, cfg.Username, cfg.Password, cfg.Workers)
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
	return a.container, a.cfg.Path
}

func (a *V3ioAdapter) connect() error {

	fullpath := pathUtil.Join(a.cfg.V3ioUrl, a.cfg.Container, a.cfg.Path)
	resp, err := a.container.Sync.GetObject(&v3io.GetObjectInput{Path: pathUtil.Join(a.cfg.Path, config.SCHEMA_CONFIG)})
	if err != nil {
		return errors.Wrap(err, "Failed to read schema at path: "+fullpath)
	}

	schema := config.Schema{}
	err = json.Unmarshal(resp.Body(), &schema)
	if err != nil {
		return errors.Wrap(err, "Failed to Unmarshal schema at path: "+fullpath)
	}

	a.partitionMngr, err = partmgr.NewPartitionMngr(&schema, a.cfg.Path, a.container)
	if err != nil {
		return errors.Wrap(err, "Failed to init DB partition manager at path: "+fullpath)
	}
	err = a.partitionMngr.Init()
	if err != nil {
		return errors.Wrap(err, "Failed to init DB partition manager at path: "+fullpath)
	}

	msg := "Starting V3IO TSDB client, server is at : " + fullpath
	a.logger.Info(msg)

	return nil
}

func (a *V3ioAdapter) InitAppenderCache() error {
	if a.MetricsCache == nil {
		a.MetricsCache = appender.NewMetricsCache(a.container, a.logger, a.cfg, a.partitionMngr)
		return a.MetricsCache.Start()
	}

	return nil
}

// Create an appender interface, for writing metrics
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

// create a querier interface, used for time series queries
func (a *V3ioAdapter) Querier(_ context.Context, mint, maxt int64) (*querier.V3ioQuerier, error) {
	if maxt < mint {
		return nil, errors.Errorf("End time %d is lower than start time %d", maxt, mint)
	}
	return querier.NewV3ioQuerier(a.container, a.logger, mint, maxt, a.cfg, a.partitionMngr), nil
}

func (a *V3ioAdapter) DeleteDB(configExists bool, force bool, fromTime int64, toTime int64) error {
	partitions := a.partitionMngr.PartsForRange(fromTime, toTime)
	for _, part := range partitions {
		a.logger.Info("Delete partition %s", part.GetTablePath())
		err := utils.DeleteTable(a.logger, a.container, part.GetTablePath(), "", a.cfg.QryWorkers)
		if err != nil && !force {
			return errors.Wrap(err, "Failed to delete partition "+part.GetTablePath())
		}
		// delete the Directory object
		err = a.container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: part.GetTablePath()})
		if err != nil && !force {
			return errors.Wrap(err, "Failed to delete partition object "+part.GetTablePath())
		}
	}
	a.partitionMngr.DeletePartitionsFromSchema(partitions)

	if len(a.partitionMngr.GetPartitionsPaths()) == 0 {
		path := a.cfg.Path + "/names/"
		a.logger.Info("Delete metric names in path %s", path)
		err := utils.DeleteTable(a.logger, a.container, path, "", a.cfg.QryWorkers)
		if err != nil && !force {
			return errors.Wrap(err, "Failed to delete names table")
		}
		// delete the Directory object
		err = a.container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: path})
		if err != nil && !force {
			return errors.Wrap(err, "Failed to delete table object")
		}
	}
	if configExists {
		schemaPath := pathUtil.Join(a.cfg.Path, config.SCHEMA_CONFIG)
		a.logger.Info("Delete TSDB config in path %s", schemaPath)
		err := a.container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: schemaPath})
		if err != nil && !force {
			return errors.New("Cant delete config or not found in " + schemaPath)
		}
		// delete the Directory object
		err = a.container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: a.cfg.Path + "/"})
		if err != nil && !force {
			return errors.Wrap(err, "Failed to delete table object")
		}
	}

	return nil
}

// return number of objects in a table
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
			return count, errors.Wrap(iter.Err(), "failed on count iterator")
		}
	}

	return count, nil
}

type v3ioAppender struct {
	metricsCache *appender.MetricsCache
}

// Add t/v value to metric and return refID (for AddFast)
func (a v3ioAppender) Add(lset utils.Labels, t int64, v float64) (uint64, error) {
	return a.metricsCache.Add(lset, t, v)
}

// faster Add using refID obtained from Add (avoid some hash/lookup overhead)
func (a v3ioAppender) AddFast(lset utils.Labels, ref uint64, t int64, v float64) error {
	return a.metricsCache.AddFast(ref, t, v)
}

// wait for completion of all updates
func (a v3ioAppender) WaitForCompletion(timeout time.Duration) (int, error) {
	return a.metricsCache.WaitForCompletion(timeout)
}

// in V3IO all ops a committed (no client cache)
func (a v3ioAppender) Commit() error   { return nil }
func (a v3ioAppender) Rollback() error { return nil }

// Appender interface provides batched appends against a storage.
type Appender interface {
	Add(l utils.Labels, t int64, v float64) (uint64, error)
	AddFast(l utils.Labels, ref uint64, t int64, v float64) error
	WaitForCompletion(timeout time.Duration) (int, error)
	Commit() error
	Rollback() error
}
