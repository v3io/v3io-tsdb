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

package appender

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"sync"
	"time"
)

// to add, rollups policy (cnt, sum, min/max, sum^2) + interval , or policy in per name lable
type MetricState struct {
	sync.RWMutex
	Lset  utils.LabelsIfc
	key   string
	name  string
	hash  uint64
	refId uint64

	store      *chunkStore
	err        error
	retryCount uint8
	newName    bool
}

const MAX_WRITE_RETRY = 2
const CHAN_SIZE = 4096

func (m *MetricState) Err() error {
	m.RLock()
	defer m.RUnlock()
	return m.err
}

// store the state and metadata for all the metrics
type MetricsCache struct {
	cfg           *config.V3ioConfig
	partitionMngr *partmgr.PartitionManager
	mtx           sync.RWMutex
	container     *v3io.Container
	logger        logger.Logger
	started       bool

	responseChan    chan *v3io.Response
	getRespChan     chan *v3io.Response
	nameUpdateChan  chan *v3io.Response
	asyncAppendChan chan *asyncAppend
	updatesInFlight int

	lastMetric     uint64
	cacheMetricMap map[uint64]*MetricState // TODO: maybe use hash as key & combine w ref
	cacheRefMap    map[uint64]*MetricState // TODO: maybe turn to list + free list, periodically delete old matrics

	NameLabelMap map[string]bool // temp store all lable names
}

func NewMetricsCache(container *v3io.Container, logger logger.Logger, cfg *config.V3ioConfig,
	partMngr *partmgr.PartitionManager) *MetricsCache {
	newCache := MetricsCache{container: container, logger: logger, cfg: cfg, partitionMngr: partMngr}
	newCache.cacheMetricMap = map[uint64]*MetricState{}
	newCache.cacheRefMap = map[uint64]*MetricState{}

	newCache.responseChan = make(chan *v3io.Response, CHAN_SIZE)
	newCache.getRespChan = make(chan *v3io.Response, CHAN_SIZE)
	newCache.nameUpdateChan = make(chan *v3io.Response, CHAN_SIZE)
	newCache.asyncAppendChan = make(chan *asyncAppend, CHAN_SIZE)

	newCache.NameLabelMap = map[string]bool{}
	return &newCache
}

type asyncAppend struct {
	metric *MetricState
	t      int64
	v      interface{}
}

func (mc *MetricsCache) StartIfNeeded() error {
	if !mc.started {
		err := mc.start()
		if err != nil {
			return errors.Wrap(err, "Failed to start Appender loop")
		}
		mc.started = true
	}

	return nil
}

// loop for handling metric events (appends and Get/Update DB responses)
// TODO: we can use multiple Go routines and spread the metrics across based on Hash LSB
func (mc *MetricsCache) start() error {

	go func() {
		for {
			select {

			case resp := <-mc.responseChan:
				// Handle V3io update expression responses

				mc.updatesInFlight--
				metric, ok := resp.Context.(*MetricState)
				respErr := resp.Error

				if respErr != nil {
					mc.logger.ErrorWith("failed v3io Update request", "metric", resp.ID, "err", respErr,
						"request", *resp.Request().Input.(*v3io.UpdateItemInput).Expression, "key",
						resp.Request().Input.(*v3io.UpdateItemInput).Path)
					// TODO: how to handle further ?
				} else {
					mc.logger.DebugWith("Process Update resp", "id", resp.ID,
						"request", *resp.Request().Input.(*v3io.UpdateItemInput).Expression, "key",
						resp.Request().Input.(*v3io.UpdateItemInput).Path)
				}

				resp.Release()

				if ok {
					// process the response and initialize a new request to update uncommitted samples
					metric.Lock()
					if respErr == nil {
						// Set fields so next write will not include redundant info (bytes, lables, init_array)
						metric.store.ProcessWriteResp()
					} else {
						metric.retryCount++
						if metric.retryCount == MAX_WRITE_RETRY {
							metric.err = errors.Wrap(respErr, "chunk update failed")
						}
					}

					err := metric.store.WriteChunks(mc, metric)
					if err != nil {
						mc.logger.ErrorWith("Submit failed", "metric", metric.Lset, "err", err)
						metric.err = errors.Wrap(err, "chunk write submit failed")
					}

					metric.Unlock()

				} else {
					mc.logger.ErrorWith("Resp doesnt have a metric pointer", "id", resp.ID)
				}

			case resp := <-mc.nameUpdateChan:
				// Handle V3io putItem in names table

				metric, ok := resp.Context.(*MetricState)
				if ok {
					metric.Lock()
					if resp.Error != nil {
						mc.logger.ErrorWith("Process Update name failed", "id", resp.ID, "name", metric.name)
					} else {
						mc.logger.DebugWith("Process Update name resp", "id", resp.ID, "name", metric.name)
					}
					metric.Unlock()
				}

				resp.Release()

			case app := <-mc.asyncAppendChan:
				// Handle append requests (Add / AddFast)

				metric := app.metric
				metric.Lock()

				// if its the first Append we need to get the metric state from the DB
				if metric.store.GetState() == storeStateInit {
					err := metric.store.GetChunksState(mc, metric, app.t)
					if err != nil {
						metric.err = err
					}
				}

				metric.store.Append(app.t, app.v)

				if metric.store.IsReady() {
					// if there are no in flight requests, update the DB
					err := metric.store.WriteChunks(mc, metric)
					if err != nil {
						mc.logger.ErrorWith("Async Submit failed", "metric", metric.Lset, "err", err)
						metric.err = err
					}
				}
				metric.Unlock()

			case resp := <-mc.getRespChan:
				// Handle V3io GetItem responses

				metric, ok := resp.Context.(*MetricState)
				respErr := resp.Error

				if respErr != nil {
					mc.logger.DebugWith("failed v3io GetItem request", "metric", resp.ID, "err", respErr,
						"key", resp.Request().Input.(*v3io.GetItemInput).Path)
				} else {
					mc.logger.DebugWith("Process GetItem resp", "id", resp.ID,
						"key", resp.Request().Input.(*v3io.GetItemInput).Path)
				}

				if ok {
					// process the Get response (update metric state) and commit pending samples to the DB
					metric.Lock()
					metric.store.ProcessGetResp(mc, metric, resp)

					if metric.store.IsReady() {
						// if there are no in flight requests, update the DB
						err := metric.store.WriteChunks(mc, metric)
						if err != nil {
							mc.logger.ErrorWith("Async Submit failed", "metric", metric.Lset, "err", err)
							metric.err = err
						}
					}

					metric.Unlock()
				} else {
					mc.logger.ErrorWith("GetItem Req ID not found", "id", resp.ID)
				}

				resp.Release()

			}
		}
	}()

	return nil
}

// return metric struct by key
func (mc *MetricsCache) getMetric(hash uint64) (*MetricState, bool) {
	mc.mtx.RLock()
	defer mc.mtx.RUnlock()

	metric, ok := mc.cacheMetricMap[hash]
	return metric, ok
}

// create a new metric and save in the map
func (mc *MetricsCache) addMetric(hash uint64, name string, metric *MetricState) {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	mc.lastMetric++
	metric.refId = mc.lastMetric
	mc.cacheRefMap[mc.lastMetric] = metric
	mc.cacheMetricMap[hash] = metric
	if _, ok := mc.NameLabelMap[name]; !ok {
		metric.newName = true
		mc.NameLabelMap[name] = true
	}
}

// return metric struct by refID
func (mc *MetricsCache) getMetricByRef(ref uint64) (*MetricState, bool) {
	mc.mtx.RLock()
	defer mc.mtx.RUnlock()

	metric, ok := mc.cacheRefMap[ref]
	return metric, ok
}

// Push append to async channel
func (mc *MetricsCache) appendTV(metric *MetricState, t int64, v interface{}) {
	mc.asyncAppendChan <- &asyncAppend{metric: metric, t: t, v: v}
}

// First time add time & value to metric (by label set)
func (mc *MetricsCache) Add(lset utils.LabelsIfc, t int64, v interface{}) (uint64, error) {

	name, key, hash := lset.GetKey()
	//hash := lset.Hash()
	metric, ok := mc.getMetric(hash)

	if ok {
		err := metric.Err()
		if err != nil {
			return 0, err
		}
		mc.appendTV(metric, t, v)
		return metric.refId, nil
	}

	metric = &MetricState{Lset: lset, key: key, name: name, hash: hash}
	metric.store = NewChunkStore()
	mc.addMetric(hash, name, metric)

	// push new/next update
	mc.appendTV(metric, t, v)
	return metric.refId, nil
}

// fast Add to metric (by refId)
func (mc *MetricsCache) AddFast(ref uint64, t int64, v interface{}) error {

	metric, ok := mc.getMetricByRef(ref)
	if !ok {
		mc.logger.ErrorWith("Ref not found", "ref", ref)
		return fmt.Errorf("ref not found")
	}

	err := metric.Err()
	if err != nil {
		return err
	}
	mc.appendTV(metric, t, v)
	return nil

}

func (mc *MetricsCache) WaitForReady(ref uint64) error {
	metric, ok := mc.getMetricByRef(ref)
	if !ok {
		mc.logger.ErrorWith("Ref not found", "ref", ref)
		return fmt.Errorf("ref not found")
	}

	for i := 0; i < 100; i++ {
		metric.RLock()
		err := metric.err
		ready := metric.store.IsReady()
		metric.RUnlock()

		if err != nil {
			return errors.Wrap(err, "metric error")
		}

		if ready {
			return nil
		}

		time.Sleep(time.Millisecond * 5)
	}

	return fmt.Errorf("Timeout waiting for metric to be ready")
}
