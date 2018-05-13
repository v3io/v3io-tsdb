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
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-tsdb/pkg/appender"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/querier"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"time"
)

const DB_VERSION = "1.0"

type V3ioAdapter struct {
	startTimeMargin int64
	logger          logger.Logger
	container       *v3io.Container
	MetricsCache    *appender.MetricsCache
	cfg             *config.V3ioConfig
	partitionMngr   *partmgr.PartitionManager
}

func CreateTSDB(v3iocfg *config.V3ioConfig, dbconfig *config.DBPartConfig) error {

	logger, _ := utils.NewLogger(v3iocfg.Verbose)
	container, err := utils.CreateContainer(logger, v3iocfg.V3ioUrl, v3iocfg.Container, v3iocfg.Workers)
	if err != nil {
		return errors.Wrap(err, "Failed to create data container")
	}

	dbconfig.Signature = "TSDB"
	dbconfig.Version = DB_VERSION

	data, err := json.Marshal(dbconfig)
	if err != nil {
		return errors.Wrap(err, "Failed to Marshal DB config")
	}

	// check if the config file already exist, abort if it does
	_, err = container.Sync.GetObject(&v3io.GetObjectInput{Path: v3iocfg.Path + "/dbconfig.json"})
	if err == nil {
		return fmt.Errorf("TSDB already exist in path: " + v3iocfg.Path)
	}

	err = container.Sync.PutObject(&v3io.PutObjectInput{Path: v3iocfg.Path + "/dbconfig.json", Body: data})

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
			cfg.V3ioUrl, cfg.Container, cfg.Workers)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create V3IO data container")
		}
	}

	err = newV3ioAdapter.connect()

	return &newV3ioAdapter, err
}

func (a *V3ioAdapter) GetDBConfig() *config.DBPartConfig {
	return a.partitionMngr.GetConfig()
}

func (a *V3ioAdapter) connect() error {

	fullpath := a.cfg.V3ioUrl + "/" + a.cfg.Container + "/" + a.cfg.Path
	resp, err := a.container.Sync.GetObject(&v3io.GetObjectInput{Path: a.cfg.Path + "/dbconfig.json"})
	if err != nil {
		return errors.Wrap(err, "Failed to read DB config at path: "+fullpath)
	}

	dbcfg := config.DBPartConfig{}
	err = json.Unmarshal(resp.Body(), &dbcfg)
	if err != nil {
		return errors.Wrap(err, "Failed to Unmarshal DB config at path: "+fullpath)
	}

	if dbcfg.Signature != "TSDB" {
		return fmt.Errorf("Bad TSDB signature at path %s", fullpath)
	}

	a.partitionMngr = partmgr.NewPartitionMngr(&dbcfg, a.cfg.Path)
	err = a.partitionMngr.Init()
	if err != nil {
		return errors.Wrap(err, "Failed to init DB partition manager at path: "+fullpath)
	}

	msg := "Starting V3IO TSDB client, server is at : " + fullpath
	fmt.Printf("\n%s\n\n", msg)
	a.logger.Info(msg)

	a.MetricsCache = appender.NewMetricsCache(a.container, a.logger, a.cfg, a.partitionMngr)

	return nil
}

// Create an appender interface, for writing metrics
func (a *V3ioAdapter) Appender() (Appender, error) {
	err := a.MetricsCache.StartIfNeeded()
	if err != nil {
		return nil, err
	}

	newAppender := v3ioAppender{metricsCache: a.MetricsCache}
	return newAppender, nil
}

func (a *V3ioAdapter) StartTime() (int64, error) {
	startTime := int64(time.Now().Unix() * 1000)
	return startTime + a.startTimeMargin, nil
}

func (a *V3ioAdapter) Close() error {
	return nil
}

// create a querier interface, used for time series queries
func (a *V3ioAdapter) Querier(_ context.Context, mint, maxt int64) (*querier.V3ioQuerier, error) {
	return querier.NewV3ioQuerier(a.container, a.logger, mint, maxt, a.cfg, a.partitionMngr), nil
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

// faster Add using refID obtained from Add (avoid some hash/lookup overhead)
func (a v3ioAppender) WaitForReady(ref uint64) error {
	return a.metricsCache.WaitForReady(ref)
}

// in V3IO all ops a committed (no client cache)
func (a v3ioAppender) Commit() error   { return nil }
func (a v3ioAppender) Rollback() error { return nil }

// Appender interface provides batched appends against a storage.
type Appender interface {
	Add(l utils.Labels, t int64, v float64) (uint64, error)
	AddFast(l utils.Labels, ref uint64, t int64, v float64) error
	WaitForReady(ref uint64) error
	Commit() error
	Rollback() error
}
