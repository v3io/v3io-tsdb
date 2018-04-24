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
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"sort"
)

var MaxArraySize = 1024

const MAX_LATE_WRITE = 59 * 3600 * 1000 // max late arrival of 59min

func NewChunkStore() *chunkStore {
	store := chunkStore{}
	store.chunks[0] = &attrAppender{}
	store.chunks[1] = &attrAppender{}
	return &store
}

// store latest + previous chunks and their state
type chunkStore struct {
	state    storeState
	curChunk int
	lastTid  int
	chunks   [2]*attrAppender

	aggrList *aggregate.AggregatorList
	pending  pendingList
	maxTime  int64
}

// chunk appender
type attrAppender struct {
	appender  chunkenc.Appender
	partition *partmgr.DBPartition
	updMarker int
	updCount  int
	chunkMint int64
	writing   bool
}

func (a *attrAppender) initialize(partition *partmgr.DBPartition, t int64) {
	a.updCount = 0
	a.updMarker = 0
	a.writing = false
	a.partition = partition
	a.chunkMint = partition.GetChunkMint(t)
}

func (a *attrAppender) inRange(t int64) bool {
	return a.partition.InChunkRange(a.chunkMint, t)
}

func (a *attrAppender) isAhead(t int64) bool {
	return a.partition.IsAheadOfChunk(a.chunkMint, t)
}

// TODO: change appender from float to interface (allow map[str]interface cols)
func (a *attrAppender) appendAttr(t int64, v interface{}) {
	a.appender.Append(t, v.(float64))
}

type pendingData struct {
	t int64
	v interface{}
}

type pendingList []pendingData

func (l pendingList) Len() int           { return len(l) }
func (l pendingList) Less(i, j int) bool { return l[i].t < l[j].t }
func (l pendingList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

type storeState uint8

const (
	storeStateInit   storeState = 0
	storeStateGet    storeState = 1 // Getting old state from storage
	storeStateReady  storeState = 2 // Ready to update
	storeStateUpdate storeState = 3 // Update/write in progress
	storeStateSort   storeState = 3 // TBD sort chunk(s) in case of late arrivals
)

func (cs *chunkStore) IsReady() bool {
	return cs.state == storeStateReady
}

func (cs *chunkStore) GetState() storeState {
	return cs.state
}

func (cs *chunkStore) SetState(state storeState) {
	cs.state = state
}

func (cs *chunkStore) GetMetricPath(metric *MetricState, basePath, table string) string {
	return fmt.Sprintf("%s/%s.%d", basePath, metric.name, metric.hash) // TODO: use TableID
}

// Read (Async) the current chunk state and data from the storage
func (cs *chunkStore) GetChunksState(mc *MetricsCache, metric *MetricState, t int64) error {

	// clac table mint
	// Get record meta + relevant attr
	// return get resp id & err

	part := mc.partitionMngr.TimeToPart(t)
	cs.chunks[0].initialize(part, t)
	chunk := chunkenc.NewXORChunk() // TODO: init based on schema, use init function
	app, _ := chunk.Appender()
	cs.chunks[0].appender = app

	cs.aggrList = aggregate.NewAggregatorList(part.AggrType())

	if mc.cfg.OverrideOld {

		cs.state = storeStateReady
		return nil
	}

	path := cs.GetMetricPath(metric, mc.cfg.Path, part.GetPath())
	getInput := v3io.GetItemInput{
		Path: path, AttributeNames: []string{"_maxtime", "_meta"}}

	request, err := mc.container.GetItem(&getInput, mc.getRespChan)
	if err != nil {
		mc.logger.ErrorWith("GetItem Failed", "metric", metric.key, "err", err)
		return err
	}

	mc.logger.DebugWith("GetItems", "name", metric.name, "key", metric.key, "reqid", request.ID)
	mc.rmapMtx.Lock()
	defer mc.rmapMtx.Unlock()
	mc.requestsMap[request.ID] = metric

	cs.state = storeStateGet
	return nil

}

// Process the GetItem response from the storage and initialize or restore the current chunk
func (cs *chunkStore) ProcessGetResp(mc *MetricsCache, metric *MetricState, resp *v3io.Response) {
	// init chunk from resp (attr)
	// append last t/v into the chunk and clear pending

	cs.state = storeStateReady

	if resp.Error != nil {
		// assume the item not found   TODO: check error code
		return
	}

	item := resp.Output.(*v3io.GetItemOutput).Item
	var maxTime int64
	val := item["_maxtime"]
	if val != nil {
		maxTime = int64(val.(int))
	}
	mc.logger.DebugWith("Got Item", "name", metric.name, "key", metric.key, "maxt", maxTime)

	// TODO: using blob append, any implications ?

	// set Last TableId, no need to create metric object
	cs.lastTid = cs.chunks[0].partition.GetId()

}

// Append data to the right chunk and table based on the time and state
func (cs *chunkStore) Append(t int64, v interface{}) {

	cs.pending = append(cs.pending, pendingData{t: t, v: v}) // TODO make it prime option

}

func (cs *chunkStore) chunkByTime(t int64) *attrAppender {

	cur := cs.chunks[cs.curChunk]
	if cur.inRange(t) {
		return cur
	}

	if cur.isAhead(t) {
		// time is ahead of this chunk time, advance cur chunk
		part := cur.partition
		cur = cs.chunks[cs.curChunk^1]

		chunk := chunkenc.NewXORChunk() // TODO: init based on schema, use init function
		app, err := chunk.Appender()
		if err != nil {
			return nil
		}
		cur.initialize(part.NextPart(t), t) // TODO: next part
		cur.appender = app
		cs.curChunk = cs.curChunk ^ 1

		return cur
	}

	prev := cs.chunks[cs.curChunk^1]
	// delayed Appends only allowed to previous chunk or within allowed window
	if prev.inRange(t) && t > cs.maxTime-MAX_LATE_WRITE {
		return prev
	}

	return nil
}

func (cs *chunkStore) WriteChunks(mc *MetricsCache, metric *MetricState) error {

	if len(cs.pending) == 0 {
		return nil
	}

	expr := ""
	notInitialized := false //TODO: init depend on get
	sort.Sort(cs.pending)

	t0 := cs.pending[0].t
	partition := mc.partitionMngr.TimeToPart(t0)
	if partition.GetId() > cs.lastTid {
		notInitialized = true
		cs.lastTid = partition.GetId()
	}

	bucket := partition.Time2Bucket(t0)
	numBuckets := partition.AggrBuckets()
	maxBucket := partition.Time2Bucket(cs.maxTime)
	isNewBucket := bucket > maxBucket
	activeChunk := cs.chunkByTime(t0)

	for i := 0; i < len(cs.pending); i++ {

		// advance maximum time processed in metric
		if cs.pending[i].t > cs.maxTime {
			cs.maxTime = cs.pending[i].t
		}

		// add value to aggregators
		cs.aggrList.Aggregate(cs.pending[i].v.(float64)) // TODO only do aggr for float types

		// add value to compressed raw value chunk
		if activeChunk != nil {
			activeChunk.appendAttr(cs.pending[i].t, cs.pending[i].v.(float64))
		}

		// if the last item or last item in the same partition add expressions and break
		if (i == len(cs.pending)-1) || !partition.InRange(cs.pending[i+1].t) {
			expr = expr + cs.aggrList.SetOrUpdateExpr("v", bucket, isNewBucket)
			expr = expr + cs.appendExpression(activeChunk, mc.cfg.ArraySize)
			break
		}

		// if the next item is in new Aggregate bucket, gen expression and init new bucket
		nextT := cs.pending[i+1].t
		nextBucket := partition.Time2Bucket(nextT)
		if nextBucket != bucket {
			expr = expr + cs.aggrList.SetOrUpdateExpr("v", bucket, isNewBucket)
			cs.aggrList.Clear()
			bucket = nextBucket
			isNewBucket = true
		}

		// if the next item is in a new chuck, gen expression and init new chunk
		if activeChunk != nil && !activeChunk.inRange(nextT) {
			expr = expr + cs.appendExpression(activeChunk, mc.cfg.ArraySize)
			activeChunk = cs.chunkByTime(nextT)
		}
	}

	cs.aggrList.Clear()
	cs.pending = cs.pending[:0] // TODO: leave pending from newer partitions if any !

	if expr == "" {
		return nil
	}

	cs.state = storeStateUpdate

	// if the table object wasnt initialized, insert init expression
	if notInitialized { // TODO: not correct, need to init once per metric/partition, maybe use cond expressions
		lblexpr := metric.Lset.GetExpr()

		// init aggregate arrays
		lblexpr = lblexpr + cs.aggrList.InitExpr("v", numBuckets)

		expr = lblexpr + fmt.Sprintf("_lset='%s'; _meta=init_array(%d,'int'); ",
			metric.key, 24) + expr // TODO: compute meta arr size
	}

	//fmt.Println("\nEXPR", expr)
	// Call V3IO async Update Item method
	expr += fmt.Sprintf("_maxtime=%d;", cs.maxTime)   // TODO: use max() expr
	path := cs.GetMetricPath(metric, mc.cfg.Path, "") // TODO: use TableID
	request, err := mc.container.UpdateItem(&v3io.UpdateItemInput{Path: path, Expression: &expr}, mc.responseChan)
	if err != nil {
		mc.logger.ErrorWith("UpdateItem Failed", "err", err)
		return err
	}

	// add async request ID to the requests map (can be avoided if V3IO will add user data in request)
	mc.logger.DebugWith("updateMetric expression", "name", metric.name, "key", metric.key, "expr", expr, "reqid", request.ID)
	mc.rmapMtx.Lock()
	defer mc.rmapMtx.Unlock()
	mc.requestsMap[request.ID] = metric

	return nil
}

// Process the (async) response for the chunk update request
func (cs *chunkStore) ProcessWriteResp() {

	for _, chunk := range cs.chunks {
		if chunk.writing {
			chunk.appender.Chunk().MoveOffset(uint16(chunk.updMarker))
			chunk.writing = false
		}
	}

	cs.state = storeStateReady

}

func (cs *chunkStore) appendExpression(chunk *attrAppender, maxArray int) string {

	if chunk != nil {
		samples := chunk.appender.Chunk().NumSamples()
		meta, offsetByte, bytes := chunk.appender.Chunk().GetChunkBuffer()
		chunk.updMarker = ((offsetByte + len(bytes) - 1) / 8) * 8
		chunk.updCount = samples
		chunk.writing = true

		expr := ""
		offset := offsetByte / 8
		ui := chunkenc.ToUint64(bytes)
		idx := chunk.partition.TimeToChunkId(chunk.chunkMint) // TODO: add DaysPerObj from part manager
		attr := chunk.partition.ChunkID2Attr("v", idx)

		if offsetByte == 0 {
			expr = expr + fmt.Sprintf("%s=init_array(%d,'int'); ", attr, maxArray)
		}

		expr = expr + fmt.Sprintf("_meta[%d]=%d; ", idx, meta) // TODO: meta name as col variable
		for i := 0; i < len(ui); i++ {
			expr = expr + fmt.Sprintf("%s[%d]=%d; ", attr, offset, int64(ui[i]))
			offset++
		}

		return expr

	}

	return ""
}
