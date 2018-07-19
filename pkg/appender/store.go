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
	"encoding/base64"
	"fmt"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"sort"
)

const MAX_LATE_WRITE = 59 * 3600 * 1000 // max late arrival of 59min

// create a chunk store with two chunks (current, previous)
func NewChunkStore() *chunkStore {
	store := chunkStore{}
	store.chunks[0] = &attrAppender{}
	store.chunks[1] = &attrAppender{}
	return &store
}

// chunkStore store state & latest + previous chunk appenders
type chunkStore struct {
	state    storeState
	curChunk int
	lastTid  int
	chunks   [2]*attrAppender

	aggrList      *aggregate.AggregatorList
	pending       pendingList
	maxTime       int64
	initMaxTime   int64 // max time read from DB metric before first append
	DelRawSamples bool  // TODO: for metrics w aggregates only
}

// Store states
type storeState uint8

const (
	storeStateInit   storeState = 0
	storeStateGet    storeState = 1 // Getting old state from storage
	storeStateReady  storeState = 2 // Ready to update
	storeStateUpdate storeState = 3 // Update/write in progress
	storeStateSort   storeState = 3 // TBD sort chunk(s) in case of late arrivals
)

// chunk appender object, state used for appending t/v to a chunk
type attrAppender struct {
	state     chunkState
	appender  chunkenc.Appender
	partition *partmgr.DBPartition
	chunkMint int64
}

type chunkState uint8

const (
	chunkStateFirst     chunkState = 1
	chunkStateMerge     chunkState = 2
	chunkStateCommitted chunkState = 4
	chunkStateWriting   chunkState = 8
)

// initialize/clear the chunk appender
func (a *attrAppender) initialize(partition *partmgr.DBPartition, t int64) {
	a.state = 0
	a.partition = partition
	a.chunkMint = partition.GetChunkMint(t)
}

// check if the time is within the chunk range
func (a *attrAppender) inRange(t int64) bool {
	return a.partition.InChunkRange(a.chunkMint, t)
}

// check if the time is ahead of the chunk range
func (a *attrAppender) isAhead(t int64) bool {
	return a.partition.IsAheadOfChunk(a.chunkMint, t)
}

// Append a single t/v to a chunk
// TODO: change appender from float to interface (allow map[str]interface cols)
func (a *attrAppender) appendAttr(t int64, v interface{}) {
	a.appender.Append(t, v.(float64))
}

// struct/list storing uncommitted samples, with time sorting support
type pendingData struct {
	t int64
	v interface{}
}

type pendingList []pendingData

func (l pendingList) Len() int           { return len(l) }
func (l pendingList) Less(i, j int) bool { return l[i].t < l[j].t }
func (l pendingList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

// store is ready to update samples into the DB
func (cs *chunkStore) IsReady() bool {
	return cs.state == storeStateReady
}

func (cs *chunkStore) GetState() storeState {
	return cs.state
}

// return the DB path for storing the metric
func (cs *chunkStore) GetMetricPath(metric *MetricState, tablePath string) string {
	return fmt.Sprintf("%s%s.%016x", tablePath, metric.name, metric.hash) // TODO: use TableID
}

// Read (Async) the current chunk state and data from the storage, used in the first chunk access
func (cs *chunkStore) GetChunksState(mc *MetricsCache, metric *MetricState, t int64) error {

	// init chunk and create aggregation list object based on partition policy
	part := mc.partitionMngr.TimeToPart(t)
	cs.chunks[0].initialize(part, t)
	cs.aggrList = aggregate.NewAggregatorList(part.AggrType())

	// TODO: if policy to merge w old chunks need to get prev chunk, vs restart appender

	// issue DB GetItem command to load last state of metric
	path := cs.GetMetricPath(metric, part.GetPath())
	getInput := v3io.GetItemInput{
		Path: path, AttributeNames: []string{"_maxtime"}}

	request, err := mc.container.GetItem(&getInput, metric, mc.getRespChan)
	if err != nil {
		mc.logger.ErrorWith("GetItem Failed", "metric", metric.key, "err", err)
		return err
	}

	mc.logger.DebugWith("GetItems", "name", metric.name, "key", metric.key, "reqid", request.ID)

	cs.state = storeStateGet
	return nil

}

// Process the GetItem response from the DB and initialize or restore the current chunk
func (cs *chunkStore) ProcessGetResp(mc *MetricsCache, metric *MetricState, resp *v3io.Response) {

	// TODO: init based on schema, use init function, recover old state vs append based on policy
	chunk := chunkenc.NewXORChunk()
	app, _ := chunk.Appender()
	cs.chunks[0].appender = app
	cs.chunks[0].state |= chunkStateFirst

	cs.state = storeStateReady

	if resp.Error != nil {
		// assume the item not found   TODO: check error code

		if metric.newName {
			path := mc.cfg.Path + "/names/" + metric.name
			putInput := v3io.PutItemInput{Path: path, Attributes: map[string]interface{}{}}

			request, err := mc.container.PutItem(&putInput, metric, mc.nameUpdateChan)
			if err != nil {
				mc.logger.ErrorWith("Update name putItem Failed", "metric", metric.key, "err", err)
			} else {
				mc.logger.DebugWith("Update name", "name", metric.name, "key", metric.key, "reqid", request.ID)
			}
		}

		return
	}

	// check and update metric Max timestamp, allow continuing from the last point in case of failure
	item := resp.Output.(*v3io.GetItemOutput).Item
	var maxTime int64
	val := item["_maxtime"]
	if val != nil {
		maxTime = int64(val.(int))
	}
	mc.logger.DebugWith("Got Item", "name", metric.name, "key", metric.key, "maxt", maxTime)

	if !mc.cfg.OverrideOld {
		cs.maxTime = maxTime
		cs.initMaxTime = maxTime
	}

	if cs.chunks[0].inRange(maxTime) && !mc.cfg.OverrideOld {
		cs.chunks[0].state |= chunkStateMerge
	}

	// set Last TableId, indicate that there is no need to create metric object
	cs.lastTid = cs.chunks[0].partition.GetId()

}

// Append data to the right chunk and table based on the time and state
func (cs *chunkStore) Append(t int64, v interface{}) {

	cs.pending = append(cs.pending, pendingData{t: t, v: v})

}

// return current, previous, or create new  chunk based on sample time
func (cs *chunkStore) chunkByTime(t int64) *attrAppender {

	// sample in the current chunk
	cur := cs.chunks[cs.curChunk]
	if cur.inRange(t) {
		return cur
	}

	// sample is in the next chunk, need to initialize
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

	// if its the first chunk after init we do not allow old updates
	if (cur.state & chunkStateFirst) != 0 {
		return nil
	}

	prev := cs.chunks[cs.curChunk^1]
	// delayed Appends only allowed to previous chunk or within allowed window
	if prev.partition != nil && prev.inRange(t) && t > cs.maxTime-MAX_LATE_WRITE {
		return prev
	}

	return nil
}

// write all pending samples to DB chunks and aggregators
func (cs *chunkStore) WriteChunks(mc *MetricsCache, metric *MetricState) error {

	if len(cs.pending) == 0 {
		return nil
	}

	expr := ""
	notInitialized := false
	sort.Sort(cs.pending)

	// init partition info and find if we need to init the metric headers (labels, ..) in case of new partition
	t0 := cs.pending[0].t
	partition := mc.partitionMngr.TimeToPart(t0)
	if partition.GetId() > cs.lastTid {
		notInitialized = true
		cs.lastTid = partition.GetId()
	}

	// init aggregation buckets info
	bucket := partition.Time2Bucket(t0)
	numBuckets := partition.AggrBuckets()
	isNewBucket := bucket > partition.Time2Bucket(cs.maxTime)

	var activeChunk *attrAppender
	var i int

	// loop over pending samples, add to chunks & aggregates (create required update expressions)
	for i < len(cs.pending) && partition.InRange(cs.pending[i].t) {

		t := cs.pending[i].t

		if t <= cs.initMaxTime && !mc.cfg.OverrideOld {
			i++
			continue
		}

		// init activeChunk if nil (when samples are too old), if still too old skip to next sample
		if activeChunk == nil {
			activeChunk = cs.chunkByTime(t)
			if activeChunk == nil {
				i++
				continue
			}
		}

		// advance maximum time processed in metric
		if t > cs.maxTime {
			cs.maxTime = t
		}

		// add value to aggregators
		cs.aggrList.Aggregate(t, cs.pending[i].v)

		// add value to compressed raw value chunk
		activeChunk.appendAttr(t, cs.pending[i].v.(float64))

		// if the last item or last item in the same partition add expressions and break
		if (i == len(cs.pending)-1) || !partition.InRange(cs.pending[i+1].t) {
			expr = expr + cs.aggrList.SetOrUpdateExpr("v", bucket, isNewBucket)
			expr = expr + cs.appendExpression(activeChunk)
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
		if !activeChunk.inRange(nextT) {
			expr = expr + cs.appendExpression(activeChunk)
			activeChunk = cs.chunkByTime(nextT)
		}

		i++
	}

	cs.aggrList.Clear()
	cs.pending = cs.pending[:0] // TODO: leave pending from newer partitions if any, use [i:] !

	if expr == "" {
		return nil
	}

	cs.state = storeStateUpdate

	// if the table object wasnt initialized, insert init expression
	if notInitialized {
		// init labels (dimension) attributes
		lblexpr := metric.Lset.GetExpr()

		// init aggregate arrays
		lblexpr = lblexpr + cs.aggrList.InitExpr("v", numBuckets)

		expr = lblexpr + fmt.Sprintf("_lset='%s'; ", metric.key) + expr
	}

	// Call V3IO async Update Item method
	expr += fmt.Sprintf("_maxtime=%d;", cs.maxTime)       // TODO: use max() expr
	path := cs.GetMetricPath(metric, partition.GetPath()) // TODO: use TableID for multi-partition
	request, err := mc.container.UpdateItem(
		&v3io.UpdateItemInput{Path: path, Expression: &expr}, metric, mc.responseChan)
	if err != nil {
		mc.logger.ErrorWith("UpdateItem Failed", "err", err)
		return err
	}

	// increase the number of in flight updates, track IO congestion
	mc.updatesInFlight++

	// add async request ID to the requests map (can be avoided if V3IO will add user data in request)
	mc.logger.DebugWith("updateMetric expression", "name", metric.name, "key", metric.key, "expr", expr, "reqid", request.ID)

	return nil
}

// Process the (async) response for the chunk update request
func (cs *chunkStore) ProcessWriteResp() {

	for _, chunk := range cs.chunks {
		// update chunk state (if it was written to)
		if chunk.state&chunkStateWriting != 0 {
			chunk.state |= chunkStateCommitted
			chunk.state &^= chunkStateWriting
			chunk.appender.Chunk().Clear()
		}
	}

	cs.state = storeStateReady

}

// return the chunk update expression
func (cs *chunkStore) appendExpression(chunk *attrAppender) string {

	if chunk != nil {
		bytes := chunk.appender.Chunk().Bytes()
		chunk.state |= chunkStateWriting

		expr := ""
		idx := chunk.partition.TimeToChunkId(chunk.chunkMint) // TODO: add DaysPerObj from part manager
		attr := chunk.partition.ChunkID2Attr("v", idx)

		val := base64.StdEncoding.EncodeToString(bytes)

		// overwrite, merge, or append based on the chunk state
		if chunk.state&chunkStateCommitted != 0 {
			expr = fmt.Sprintf("%s=%s+blob('%s'); ", attr, attr, val)
		} else if chunk.state&chunkStateMerge != 0 {
			expr = fmt.Sprintf("%s=if_not_exists(%s,blob('')) + blob('%s'); ", attr, attr, val)
		} else {
			expr = fmt.Sprintf("%s=blob('%s'); ", attr, val)
		}

		return expr

	}

	return ""
}
