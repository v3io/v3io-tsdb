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

package v3io_tsdb

import (
	"context"
	"fmt"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/appender"
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-tsdb/querier"
	"github.com/v3io/v3io-tsdb/v3ioutil"
	"time"
)

type v3ioAdapter struct {
	startTimeMargin int64
	logger          logger.Logger
	container       *v3io.Container
	metricsCache    *appender.MetricsCache
	cfg             *config.TsdbConfig
}

func NewV3ioAdapter(cfg *config.TsdbConfig, container *v3io.Container, logger logger.Logger) v3ioAdapter {

	newV3ioAdapter := v3ioAdapter{}
	newV3ioAdapter.cfg = cfg
	if logger != nil {
		newV3ioAdapter.logger = logger
	} else {
		newV3ioAdapter.logger, _ = v3ioutil.NewLogger(cfg.Verbose)
	}

	if container != nil {
		newV3ioAdapter.container = container
	} else {
		newV3ioAdapter.container, _ = v3ioutil.CreateContainer(newV3ioAdapter.logger,
			cfg.V3ioUrl, cfg.Container, cfg.Workers)
	}

	newV3ioAdapter.metricsCache = appender.NewMetricsCache(newV3ioAdapter.container, newV3ioAdapter.logger, cfg)
	return newV3ioAdapter
}

func (a v3ioAdapter) Start() error {
	msg := fmt.Sprintf("Starting V3IO TSDB client, server at : %s/%s/%s",
		a.cfg.V3ioUrl, a.cfg.Container, a.cfg.Path)
	fmt.Println(msg)
	a.logger.Info(msg)
	err := a.metricsCache.Start()
	if err != nil {
		return err
	}

	_, err = a.container.Sync.ListBucket(&v3io.ListBucketInput{})
	if err != nil {
		a.logger.ErrorWith("Failed to access v3io container", "url", a.cfg.V3ioUrl, "err", err)
	}
	return errors.Wrap(err, "Failed to access v3io container")
}

func (a v3ioAdapter) Appender() (storage.Appender, error) {
	newAppender := v3ioAppender{metricsCache: a.metricsCache}
	return newAppender, nil
}

func (a v3ioAdapter) StartTime() (int64, error) {
	startTime := int64(time.Now().Unix() * 1000)
	return startTime + a.startTimeMargin, nil
}

func (a v3ioAdapter) Close() error {
	return nil
}

func (a v3ioAdapter) Querier(_ context.Context, mint, maxt int64) (storage.Querier, error) {
	return querier.NewV3ioQuerier(a.container, a.logger, mint, maxt, &a.metricsCache.NameLabelMap, a.cfg), nil
}

type v3ioAppender struct {
	metricsCache *appender.MetricsCache
}

func (a v3ioAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	return a.metricsCache.Add(lset, t, v)
}

func (a v3ioAppender) AddFast(lset labels.Labels, ref uint64, t int64, v float64) error {
	return a.metricsCache.AddFast(lset, ref, t, v)
}

func (a v3ioAppender) Commit() error   { return nil }
func (a v3ioAppender) Rollback() error { return nil }
