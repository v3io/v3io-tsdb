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
	"github.com/v3io/v3io-tsdb/config"
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

const MAX_WRITE_RETRY = 3
const CHAN_SIZE = 2048

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
	nameUpdateChan  chan *v3io.Response
	asyncAppendChan chan *asyncAppend
	updatesInFlight int

	getsInFlight    int
	extraQueue      *ElasticQueue
	updatesComplete chan int
	newUpdates      chan int

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
	newCache.nameUpdateChan = make(chan *v3io.Response, CHAN_SIZE)
	newCache.asyncAppendChan = make(chan *asyncAppend, CHAN_SIZE)

	newCache.extraQueue = NewElasticQueue()
	newCache.updatesComplete = make(chan int, 10)
	newCache.newUpdates = make(chan int, 100)

	newCache.NameLabelMap = map[string]bool{}
	return &newCache
}

type asyncAppend struct {
	metric *MetricState
	t      int64
	v      interface{}
	resp   chan int
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

const maxSampleLoop = 64

// start event loops for handling metric updates (appends and Get/Update DB responses)
// TODO: we can use multiple Go routines and spread the metrics across based on Hash LSB
func (mc *MetricsCache) start() error {

	mc.nameUpdateRespLoop()
	mc.metricsUpdateLoop(0)
	mc.metricFeed(0)

	return nil
}

func (mc *MetricsCache) metricFeed(index int) {

	go func() {
		inFlight := 0
		waitForInFlight := 0
		var completeChan chan int

		for {

			select {

			case inFlight = <-mc.updatesComplete:
				length := mc.extraQueue.Length()
				mc.logger.Debug("Complete Update cycle - inflight %d len %d\n", inFlight, length)

				if completeChan != nil && (inFlight+length) <= waitForInFlight {
					completeChan <- inFlight + length
				}

			case app := <-mc.asyncAppendChan:
				newMetrics := 0
				maxPending := 0
			inLoop:
				for i := 0; i < maxSampleLoop; i++ {

					if app.metric == nil {
						// handle Completion Notification requests (metric == nil)
						waitForInFlight = int(app.t)
						completeChan = app.resp

					} else {
						// Handle append requests (Add / AddFast)
						metric := app.metric
						metric.Lock()

						if metric.store.GetState() != storeStateError {

							metric.store.Append(app.t, app.v)
							if len(metric.store.pending) > maxPending {
								maxPending = len(metric.store.pending)
							}

							// if there are no in flight requests, add the metric to the queue and update state
							if metric.store.IsReady() || metric.store.GetState() == storeStateInit {

								if metric.store.GetState() == storeStateInit {
									metric.store.SetState(storeStatePreGet)
								}
								if metric.store.IsReady() {
									metric.store.SetState(storeStatePending)
								}

								length := mc.extraQueue.Push(metric)
								if length < 2*mc.cfg.Workers { // && waiting
									newMetrics++
								}
							}

						}
						metric.Unlock()
					}

					// poll if we have more updates (accelerate the outer select)
					select {
					case app = <-mc.asyncAppendChan:
					default:
						break inLoop
					}
				}
				if newMetrics > 0 {
					mc.newUpdates <- newMetrics
				}

				// If we have too much work, stall the queue
				if maxPending > 3*MaxSamplesInWrite {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	}()
}

func (mc *MetricsCache) postMetricUpdates(metric *MetricState) bool {

	metric.Lock()
	//fmt.Println("NEW Post ", metric.Lset, mc.updatesInFlight, len(metric.store.pending), metric.store.GetState())

	if metric.store.GetState() == storeStatePreGet {
		err := metric.store.GetChunksState(mc, metric, metric.store.pending[0].t)
		if err != nil {
			metric.err = err
		}

	} else {
		err := metric.store.WriteChunks(mc, metric)
		if err != nil {
			mc.logger.ErrorWith("Submit failed", "metric", metric.Lset, "err", err)
			metric.err = errors.Wrap(err, "chunk write submit failed")
			metric.store.SetState(storeStateError)
		}

	}
	metric.Unlock()

	return mc.updatesInFlight < mc.cfg.Workers*2
}

func (mc *MetricsCache) metricsUpdateLoop(index int) {

	go func() {

		for {

			select {

			case length := <-mc.newUpdates:
				// Handle new metric notifications (from metricFeed)
				if length+mc.updatesInFlight > mc.cfg.Workers*2 {
					length = mc.cfg.Workers*2 - mc.updatesInFlight
					if length < 0 {
						length = 0
					}
				}

				for _, metric := range mc.extraQueue.PopN(length) {
					mc.postMetricUpdates(metric)
				}

			case resp := <-mc.responseChan:
				// Handle V3io async responses

				mc.updatesInFlight--
				metric := resp.Context.(*MetricState)
				metric.Lock()

				if resp.Error != nil && metric.store.GetState() != storeStateGet {
					mc.logger.ErrorWith("failed IO", "id", resp.ID, "err", resp.Error,
						"key", metric.key, "state", metric.store.GetState())
				} else {
					mc.logger.DebugWith("IOe resp", "id", resp.ID, "err", resp.Error,
						"key", metric.key, "state", metric.store.GetState())
				}

				if metric.store.GetState() == storeStateGet {
					// Handle Get response, sync metric state with the DB
					metric.store.ProcessGetResp(mc, metric, resp)

				} else {
					// Handle Update Expression responses
					if resp.Error == nil {
						// Set fields so next write will not include redundant info (bytes, lables, init_array)
						metric.store.ProcessWriteResp()
						metric.retryCount = 0
					} else {
						// Metrics with too many update errors go into Error state
						metric.retryCount++
						if metric.retryCount == MAX_WRITE_RETRY {
							metric.err = errors.Wrap(resp.Error, "chunk update failed after few retries")
							metric.store.SetState(storeStateError)
						}
					}
				}

				resp.Release()

				length := mc.extraQueue.Length()
				//fmt.Println("RESP ", metric.Lset, mc.updatesInFlight, len(metric.store.pending), length)
				if length == 0 {
					err := metric.store.WriteChunks(mc, metric)
					if err != nil {
						mc.logger.ErrorWith("Submit failed", "metric", metric.Lset, "err", err)
						metric.err = errors.Wrap(err, "chunk write submit failed")
						metric.store.SetState(storeStateError)
					}
				} else if len(metric.store.pending) > 0 {
					metric.store.state = storeStateUpdate
					mc.extraQueue.Push(metric)
				}

				metric.Unlock()

				// If we have queued metrics and the channel has room for more updates we send them
				if length > 0 {
					if metric := mc.extraQueue.Pop(); metric != nil {
						mc.postMetricUpdates(metric)
					}
				}

				// Notify the metric feeder that all in-flight tasks are done
				if mc.updatesInFlight == 0 {
					mc.updatesComplete <- 0
				}
			}
		}
	}()

}

func (mc *MetricsCache) nameUpdateRespLoop() {

	go func() {
		for {
			resp := <-mc.nameUpdateChan
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
		}
	}()
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

	for i := 0; i < 1000; i++ {
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

func (mc *MetricsCache) WaitForCompletion(inFlight int64, timeout int) int {
	waitChan := make(chan int, 2)
	mc.asyncAppendChan <- &asyncAppend{metric: nil, t: inFlight, v: 0, resp: waitChan}
	return <-waitChan
}
