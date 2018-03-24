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
	"github.com/v3io/v3io-tsdb/utils"
	"sync"
)

// to add, rollups policy (cnt, sum, min/max, sum^2) + interval , or policy in per name lable
type MetricState struct {
	sync.RWMutex
	busyGetting bool
	Lset        labels.Labels
	key         string
	name        string
	hash        uint64

	ExpiresAt int64
	refId     uint64
	wait      chan bool
	//waiting      bool

	store *chunkStore
	err   error
}

func (m *MetricState) Err() error {
	//return nil  // TODO: temp bypass
	m.RLock()
	defer m.RUnlock()
	return m.err
}

type MetricsCache struct {
	cfg             *config.TsdbConfig
	headPartition   *utils.ColDBPartition
	mtx             sync.RWMutex
	rmapMtx         sync.RWMutex
	container       *v3io.Container
	logger          logger.Logger
	responseChan    chan *v3io.Response
	getRespChan     chan *v3io.Response
	asyncAppendChan chan *asyncAppend
	lastMetric      uint64
	cacheMetricMap  map[string]*MetricState
	cacheRefMap     map[uint64]*MetricState
	requestsMap     map[uint64]*MetricState // v3io async requests

	NameLabelMap map[string]bool
}

func NewMetricsCache(container *v3io.Container, logger logger.Logger, cfg *config.TsdbConfig) *MetricsCache {
	newCache := MetricsCache{container: container, logger: logger, cfg: cfg}
	newCache.cacheMetricMap = map[string]*MetricState{}
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
	v      float64
}

func (mc *MetricsCache) Start() error {

	go func() {
		for {
			select {

			case resp := <-mc.responseChan:
				//mc.logger.Warn("Got response ID %d", resp.ID)

				// TODO: add metric interface to v3io req instead of using req map
				mc.rmapMtx.Lock()
				metric, ok := mc.requestsMap[resp.ID]
				delete(mc.requestsMap, resp.ID)
				mc.rmapMtx.Unlock()
				respErr := resp.Error

				if respErr != nil {
					mc.logger.ErrorWith("failed v3io request", "metric", resp.ID, "err", respErr,
						"request", *resp.Request().Input.(*v3io.UpdateItemInput).Expression, "key",
						resp.Request().Input.(*v3io.UpdateItemInput).Path)
					// TODO: how to handle further ?
				} else {
					mc.logger.DebugWith("Process resp", "id", resp.ID,
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

					tableId, expr := metric.store.WriteChunks(metric)
					if tableId != -1 {
						err := mc.submitUpdate(metric, tableId, expr)
						fmt.Println("Loop:", expr)
						if err != nil {
							mc.logger.ErrorWith("Submit failed", "metric", metric.Lset, "err", err)
							metric.err = err
						}
					}

					//if metric.waiting {
					//	// if Appender is blocking (waiting for resp), release it
					//	metric.wait <- true
					//	mc.logger.DebugWith("after wait", "metric", metric)
					//	metric.waiting = false
					//}
					metric.Unlock()

				} else {
					mc.logger.ErrorWith("Req ID not found", "id", resp.ID)
				}

			case app := <-mc.asyncAppendChan:
				metric := app.metric
				metric.Lock()
				metric.store.Append(app.t, app.v)

				if metric.store.IsReady() {
					// if there are no in flight requests, update the DB
					tableId, expr := metric.store.WriteChunks(metric)
					//fmt.Println("AoW:", expr)
					if tableId != -1 {
						err := mc.submitUpdate(metric, tableId, expr)
						fmt.Println("Async:", expr)
						if err != nil {
							mc.logger.ErrorWith("Async submit failed", "metric", metric.Lset, "err", err)
							metric.err = err
						}
					}
				}
				metric.Unlock()

			}
		}
	}()

	return nil
}

// return metric struct by key
func (mc *MetricsCache) getMetric(key string) (*MetricState, bool) {
	mc.mtx.RLock()
	defer mc.mtx.RUnlock()

	metric, ok := mc.cacheMetricMap[key]
	return metric, ok
}

// create a new metric and save in the map
func (mc *MetricsCache) addMetric(key, name string, metric *MetricState) {
	mc.mtx.Lock()
	defer mc.mtx.Unlock()

	mc.lastMetric++
	metric.wait = make(chan bool)
	metric.refId = mc.lastMetric
	mc.cacheRefMap[mc.lastMetric] = metric
	mc.cacheMetricMap[key] = metric
	mc.NameLabelMap[name] = true // TODO: temporary until we get names from storage
}

// return metric struct by refID
func (mc *MetricsCache) getMetricByRef(ref uint64) (*MetricState, bool) {
	mc.mtx.RLock()
	defer mc.mtx.RUnlock()

	metric, ok := mc.cacheRefMap[ref]
	return metric, ok
}

func (mc *MetricsCache) Append(metric *MetricState, t int64, v float64) {
	mc.asyncAppendChan <- &asyncAppend{metric: metric, t: t, v: v}
}

// First time add time & value to metric (by label set)
func (mc *MetricsCache) Add(lset labels.Labels, t int64, v float64) (uint64, error) {

	name, key := labels2key(lset)
	metric, ok := mc.getMetric(key)

	if ok { //TODO: and didnt expire (beyond chunk max time)
		err := metric.Err()
		if err != nil {
			return 0, err
		}
		mc.Append(metric, t, v)
		return metric.refId, nil
	}

	metric = &MetricState{Lset: lset, key: key, name: name, hash: lset.Hash()}
	metric.store = NewChunkStore()
	metric.store.state = storeStateReady // TODO: get metric on first instead
	mc.addMetric(key, name, metric)

	// push new/next update
	mc.Append(metric, t, v)

	// TODO: get state

	return metric.refId, nil //err
}

// fast Add to metric (by refId)
func (mc *MetricsCache) AddFast(lset labels.Labels, ref uint64, t int64, v float64) error {

	metric, ok := mc.getMetricByRef(ref)
	if !ok {
		mc.logger.ErrorWith("Ref not found", "ref", ref)
		return fmt.Errorf("ref not found")
	}

	// check metric didnt expire

	// Append to metric or block (if behind)
	err := metric.Err()
	if err != nil {
		return err
	}
	mc.Append(metric, t, v)
	return nil

}

func (mc *MetricsCache) submitUpdate(metric *MetricState, tableId int, expr string) error {

	path := fmt.Sprintf("%s/%s.%d", mc.cfg.Path, metric.name, metric.Lset.Hash())
	request, err := mc.container.UpdateItem(&v3io.UpdateItemInput{Path: path, Expression: &expr}, mc.responseChan)
	if err != nil {
		mc.logger.ErrorWith("UpdateItem Failed", "err", err)
		return err
	}
	//metric.Lock()

	mc.logger.DebugWith("updateMetric expression", "name", metric.name, "key", metric.key, "expr", expr, "reqid", request.ID)

	mc.rmapMtx.Lock()
	defer mc.rmapMtx.Unlock()
	mc.requestsMap[request.ID] = metric

	return nil

}

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
	return name, key[:len(key)-1]
}
