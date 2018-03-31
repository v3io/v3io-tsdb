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
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-tsdb/partmgr"
	"sync"
	"time"
)

// to add, rollups policy (cnt, sum, min/max, sum^2) + interval , or policy in per name lable
type MetricState struct {
	sync.RWMutex
	Lset  labels.Labels
	key   string
	name  string
	hash  uint64
	refId uint64

	store *chunkStore
	err   error
}

func (m *MetricState) Err() error {
	m.RLock()
	defer m.RUnlock()
	return m.err
}

type MetricsCache struct {
	cfg           *config.TsdbConfig
	partitionMngr *partmgr.PartitionManager
	mtx           sync.RWMutex
	rmapMtx       sync.RWMutex
	container     *v3io.Container
	logger        logger.Logger

	responseChan    chan *v3io.Response
	getRespChan     chan *v3io.Response
	asyncAppendChan chan *asyncAppend

	lastMetric     uint64
	cacheMetricMap map[uint64]*MetricState // TODO: maybe use hash as key & combine w ref
	cacheRefMap    map[uint64]*MetricState // TODO: maybe turn to list + free list, periodically delete old matrics
	requestsMap    map[uint64]*MetricState // v3io async requests

	NameLabelMap map[string]bool // temp store all lable names
}

func NewMetricsCache(container *v3io.Container, logger logger.Logger, cfg *config.TsdbConfig,
	partMngr *partmgr.PartitionManager) *MetricsCache {
	newCache := MetricsCache{container: container, logger: logger, cfg: cfg, partitionMngr: partMngr}
	newCache.cacheMetricMap = map[uint64]*MetricState{}
	newCache.cacheRefMap = map[uint64]*MetricState{}
	newCache.requestsMap = map[uint64]*MetricState{}

	newCache.responseChan = make(chan *v3io.Response, 1024)
	newCache.getRespChan = make(chan *v3io.Response, 1024)
	newCache.asyncAppendChan = make(chan *asyncAppend, 1024)

	newCache.NameLabelMap = map[string]bool{}
	return &newCache
}

type asyncAppend struct {
	metric *MetricState
	t      int64
	v      interface{}
}

func (mc *MetricsCache) Start() error {

	go func() {
		for {
			select {

			case resp := <-mc.responseChan:
				// Handle V3io update expression responses

				// TODO: add metric interface to v3io req instead of using req map
				mc.rmapMtx.Lock()
				metric, ok := mc.requestsMap[resp.ID]
				delete(mc.requestsMap, resp.ID)
				mc.rmapMtx.Unlock()
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
					metric.Lock()
					if respErr == nil {
						// Set fields so next write will not include redundant info (bytes, lables, init_array)
						metric.store.ProcessWriteResp()
					}

					err := metric.store.WriteChunks(mc, metric)
					if err != nil {
						mc.logger.ErrorWith("Submit failed", "metric", metric.Lset, "err", err)
						metric.err = err
					}

					metric.Unlock()

				} else {
					mc.logger.ErrorWith("Req ID not found", "id", resp.ID)
				}

			case app := <-mc.asyncAppendChan:
				// Handle append requests (Add / AddFast)

				metric := app.metric
				metric.Lock()

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

				// TODO: add metric interface to v3io req instead of using req map
				mc.rmapMtx.Lock()
				metric, ok := mc.requestsMap[resp.ID]
				delete(mc.requestsMap, resp.ID)
				mc.rmapMtx.Unlock()
				respErr := resp.Error

				if respErr != nil {
					mc.logger.ErrorWith("failed v3io GetItem request", "metric", resp.ID, "err", respErr,
						"key", resp.Request().Input.(*v3io.GetItemInput).Path)
				} else {
					mc.logger.DebugWith("Process GetItem resp", "id", resp.ID,
						"key", resp.Request().Input.(*v3io.GetItemInput).Path)
				}

				if ok {
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
					time.Sleep(time.Second)
					panic(nil)
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
	mc.NameLabelMap[name] = true // TODO: temporary until we get names from storage
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
func (mc *MetricsCache) Add(lset labels.Labels, t int64, v interface{}) (uint64, error) {

	name, key := labels2key(lset)
	hash := lset.Hash()
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
func (mc *MetricsCache) AddFast(lset labels.Labels, ref uint64, t int64, v interface{}) error {

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

// convert Label set to a string in the form key1=v1,key2=v2..
func labels2key(lset labels.Labels) (string, string) {
	key := ""
	name := ""
	for _, lbl := range lset {
		if lbl.Name == "__name__" {
			name = lbl.Value
		} else {
			key = key + lbl.Name + "=" + lbl.Value + ","
		}
	}
	if len(key) == 0 {
		return name, ""
	}
	return name, key[:len(key)-1]
}
