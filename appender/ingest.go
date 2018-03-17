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
	"github.com/v3io/v3io-tsdb/chunkenc"
	"github.com/v3io/v3io-tsdb/config"
	"math"
	"sync"
)

const KEY_SEPERATOR = "="

// to add, rollups policy (cnt, sum, min/max, sum^2) + interval , or policy in per name lable
type MetricState struct {
	sync.RWMutex
	busyUpdating  bool
	busyGetting   bool
	initialized   bool
	samplesBehind int
	Lset          labels.Labels
	key           string
	curT          int64
	writtenChunk  int
	curChunk      int
	ExpiresAt     int64
	refId         uint64
	wait          chan bool
	waiting       bool

	chunk    chunkenc.Chunk
	appender chunkenc.Appender
	toTrim   int
}

type MetricsCache struct {
	cfg            *config.TsdbConfig
	mtx            sync.RWMutex
	rmapMtx        sync.RWMutex
	container      *v3io.Container
	logger         logger.Logger
	responseChan   chan *v3io.Response
	lastMetric     uint64
	cacheMetricMap map[string]*MetricState
	cacheRefMap    map[uint64]*MetricState
	requestsMap    map[uint64]*MetricState // v3io async requests

	NameLabelMap map[string]bool
}

func NewMetricsCache(container *v3io.Container, logger logger.Logger, cfg *config.TsdbConfig) *MetricsCache {
	newCache := MetricsCache{container: container, logger: logger, cfg: cfg}
	newCache.cacheMetricMap = map[string]*MetricState{}
	newCache.cacheRefMap = map[uint64]*MetricState{}
	newCache.requestsMap = map[uint64]*MetricState{}
	newCache.responseChan = make(chan *v3io.Response, 100)
	newCache.NameLabelMap = map[string]bool{}
	return &newCache
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
					//mc.logger.Warn("resp lock%s %d", metric.key, metric.samplesBehind)

					if respErr == nil {
						// Set fields so next write will not include redundant info (bytes, lables, init_array)
						metric.chunk.MoveOffset(uint16(metric.toTrim))
						metric.initialized = true
						metric.writtenChunk = metric.curChunk
					}

					if metric.samplesBehind > 0 {
						// if pending writes, submit them
						mc.updateMetric(metric)
					} else {
						metric.busyUpdating = false
					}

					if metric.waiting {
						// if Appender is blocking (waiting for resp), release it
						metric.wait <- true
						mc.logger.DebugWith("after wait", "metric", metric.Lset)
						metric.waiting = false
					}
					metric.samplesBehind = 0
					metric.Unlock()
					//mc.logger.Warn("resp unlock")

				} else {
					mc.logger.ErrorWith("Req ID not found", "id", resp.ID)
				}

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

// Append metric data to local cache and/or commit to v3io, block if too far behind
func (mc *MetricsCache) appendOrWait(metric *MetricState, t int64, v float64) error {
	var err error
	metric.Lock()
	mc.logger.DebugWith("appendOrWait", "behind", metric.samplesBehind, "t", t, "v", v)
	//mc.logger.Warn("AoW lock: %s %d", metric.key, metric.samplesBehind)
	metric.appender.Append(t, v)
	metric.samplesBehind++

	if metric.samplesBehind > mc.cfg.MaxBehind {
		// if too far behind, block writer
		mc.logger.WarnWith("Behind, Waiting for updates to complete", "ref", metric.refId)
		metric.waiting = true
		metric.Unlock()
		<-metric.wait
		mc.logger.Warn("after done")
		return nil
	}

	if !metric.busyUpdating {
		// if there are no in flight requests, update the DB
		err = mc.updateMetric(metric)
	}
	metric.Unlock()
	//mc.logger.Warn("AoW unlock: %d", t)
	return err
}

// First time add time & value to metric (by label set)
func (mc *MetricsCache) Add(lset labels.Labels, t int64, v float64) (uint64, error) {

	name, key := labels2key(lset)
	metric, ok := mc.getMetric(key)

	if ok { //TODO: and didnt expire (beyond chunk max time)
		err := mc.appendOrWait(metric, t, v)
		return metric.refId, err
	}

	metric = &MetricState{Lset: lset, key: key}
	metric.chunk = chunkenc.NewXORChunk(0, math.MaxInt64)
	appender, err := metric.chunk.Appender()
	if err != nil {
		return 0, err
	}
	metric.appender = appender
	metric.appender.Append(t, v)
	mc.addMetric(key, name, metric)

	// TODO: get state

	// push new/next update
	metric.samplesBehind++
	err = mc.updateMetric(metric)

	return metric.refId, err
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
	err := mc.appendOrWait(metric, t, v)
	return err

}

// Update metric to the database
func (mc *MetricsCache) updateMetric(metric *MetricState) error {
	metric.busyUpdating = true

	//mc.logger.DebugWith("updateMetric", "Lset", metric.Lset)
	expr := ""
	if !metric.initialized {
		for _, lbl := range metric.Lset {
			if lbl.Name != "__name__" {
				expr = expr + fmt.Sprintf("%s='%s'; ", lbl.Name, lbl.Value)
			} else {
				expr = expr + fmt.Sprintf("_name='%s'; ", lbl.Value)
			}
		}
	}

	//offset, toTrim, count, ui := chunkenc.ToUint64(metric.chunk)
	meta, offsetByte, b := metric.chunk.GetChunkBuffer()
	offset := offsetByte / 8
	ui := chunkenc.ToUint64(b)
	metric.toTrim = ((offsetByte + len(b) - 1) / 8) * 8
	metric.curChunk = 1 //utils.TimeToChunkId(mc.cfg, t)
	if metric.curChunk != metric.writtenChunk {
		expr = expr + fmt.Sprintf("_values=init_array(%d,'int'); ", mc.cfg.ArraySize)
	}
	expr = expr + fmt.Sprintf("_values[0]=%d; ", meta)
	for i := 0; i < len(ui); i++ {
		offset++
		expr = expr + fmt.Sprintf("_values[%d]=%d; ", offset, int64(ui[i]))
	}
	expr = expr + fmt.Sprintf("_arrlen=%d; _samples=%d", offset+len(ui), 0)

	//mc.logger.WarnWith("updateMetric", "key", metric.key)
	//metric.Unlock()
	request, err := mc.container.UpdateItem(&v3io.UpdateItemInput{
		Path: mc.cfg.Path + "/" + metric.key, Expression: &expr}, mc.responseChan)
	if err != nil {
		mc.logger.ErrorWith("UpdateItem Failed", "err", err)
		return err
	}
	//metric.Lock()

	mc.logger.DebugWith("updateMetric expression", "key", metric.key, "totrim", metric.toTrim, "count", 0, "expr", expr, "reqid", request.ID)

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
			key = key + lbl.Name + KEY_SEPERATOR + lbl.Value + KEY_SEPERATOR
		}
	}
	return name, name + "." + key[:len(key)-len(KEY_SEPERATOR)]
}
