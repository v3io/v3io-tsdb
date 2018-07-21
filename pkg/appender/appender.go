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
)

const MAX_WRITE_RETRY = 3
const CHAN_SIZE = 2048
const maxSampleLoop = 64

// to add, rollups policy (cnt, sum, min/max, sum^2) + interval , or policy in per name lable
type MetricState struct {
	sync.RWMutex
	state storeState
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

// Store states
type storeState uint8

const (
	storeStateInit    storeState = 0
	storeStatePreGet  storeState = 1 // Need to get state
	storeStateGet     storeState = 2 // Getting old state from storage
	storeStateReady   storeState = 3 // Ready to update
	storeStatePending storeState = 4 // New data for metric
	storeStateUpdate  storeState = 5 // Update/write in progress
	storeStateSort    storeState = 6 // TBD sort chunk(s) in case of late arrivals
	storeStateError   storeState = 7 // Metric in error state
)

// store is ready to update samples into the DB
func (m *MetricState) IsReady() bool {
	return m.state == storeStateReady
}

func (m *MetricState) HasError() bool {
	return m.state == storeStateError
}

func (m *MetricState) GetState() storeState {
	return m.state
}

func (m *MetricState) SetState(state storeState) {
	m.state = state
}

func (m *MetricState) SetError(err error) {
	m.SetState(storeStateError)
	m.err = err
}

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

func (mc *MetricsCache) WaitForCompletion(inFlight int64, timeout int) int {
	waitChan := make(chan int, 2)
	mc.asyncAppendChan <- &asyncAppend{metric: nil, t: inFlight, v: 0, resp: waitChan}
	return <-waitChan
}
