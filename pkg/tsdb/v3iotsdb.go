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

type V3ioAdapter struct {
	startTimeMargin int64
	logger          logger.Logger
	container       *v3io.Container
	MetricsCache    *appender.MetricsCache
	cfg             *config.TsdbConfig
	partitionMngr   *partmgr.PartitionManager
}

// Create a new TSDB Adapter, similar to Prometheus TSDB Adapter with few extensions
// Prometheus compliant Adapter is under /promtsdb
func NewV3ioAdapter(cfg *config.TsdbConfig, container *v3io.Container, logger logger.Logger) *V3ioAdapter {

	newV3ioAdapter := V3ioAdapter{}
	newV3ioAdapter.cfg = cfg
	if logger != nil {
		newV3ioAdapter.logger = logger
	} else {
		newV3ioAdapter.logger, _ = utils.NewLogger(cfg.Verbose)
	}

	if container != nil {
		newV3ioAdapter.container = container
	} else {
		newV3ioAdapter.container, _ = utils.CreateContainer(newV3ioAdapter.logger,
			cfg.V3ioUrl, cfg.Container, cfg.Workers)
	}

	return &newV3ioAdapter
}

// initialize the adapter and start event loops
func (a *V3ioAdapter) Start() error {
	msg := fmt.Sprintf("\nStarting V3IO TSDB client, server is at : %s/%s/%s\n",
		a.cfg.V3ioUrl, a.cfg.Container, a.cfg.Path)
	fmt.Println(msg)
	a.logger.Info(msg)

	a.partitionMngr = partmgr.NewPartitionMngr(a.cfg)
	err := a.partitionMngr.Init()
	if err != nil {
		return err
	}

	a.MetricsCache = appender.NewMetricsCache(a.container, a.logger, a.cfg, a.partitionMngr)

	err = a.MetricsCache.Start()
	if err != nil {
		return err
	}

	_, err = a.container.Sync.ListBucket(&v3io.ListBucketInput{})

	if err != nil {
		a.logger.ErrorWith("Failed to access v3io container", "url", a.cfg.V3ioUrl, "err", err)
		return errors.Wrap(err, "Failed to access v3io container")
	}

	return nil
}

// Create an appender interface, for writing metrics
func (a *V3ioAdapter) Appender() (Appender, error) {
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
	return querier.NewV3ioQuerier(a.container, a.logger, mint, maxt, &a.MetricsCache.NameLabelMap, a.cfg, a.partitionMngr), nil
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

// in V3IO all ops a committed (no client cache)
func (a v3ioAppender) Commit() error   { return nil }
func (a v3ioAppender) Rollback() error { return nil }

// Appender interface provides batched appends against a storage.
type Appender interface {
	Add(l utils.Labels, t int64, v float64) (uint64, error)
	AddFast(l utils.Labels, ref uint64, t int64, v float64) error
	Commit() error
	Rollback() error
}
