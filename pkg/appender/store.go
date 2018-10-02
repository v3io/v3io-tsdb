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
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"sort"
)

// TODO: make it configurable
const maxLateArrivalInterval = 59 * 60 * 1000 // max late arrival of 59min

// create a chunk store with two chunks (current, previous)
func NewChunkStore(logger logger.Logger) *chunkStore {
	store := chunkStore{logger: logger}
	store.chunks[0] = &attrAppender{}
	store.chunks[1] = &attrAppender{}
	return &store
}

// chunkStore store state & latest + previous chunk appenders
type chunkStore struct {
	logger logger.Logger

	curChunk int
	lastTid  int64
	chunks   [2]*attrAppender

	aggrList      *aggregate.AggregatorList
	pending       pendingList
	maxTime       int64
	initMaxTime   int64 // max time read from DB metric before first append
	delRawSamples bool  // TODO: for metrics w aggregates only
}

func (cs *chunkStore) samplesQueueLength() int {
	return len(cs.pending)
}

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

// Read (Async) the current chunk state and data from the storage, used in the first chunk access
func (cs *chunkStore) getChunksState(mc *MetricsCache, metric *MetricState) (bool, error) {

	if len(cs.pending) == 0 {
		return false, nil
	}
	// init chunk and create aggregation list object based on partition policy
	t := cs.pending[0].t
	part, err := mc.partitionMngr.TimeToPart(t)
	if err != nil {
		return false, err
	}
	cs.chunks[0].initialize(part, t)
	cs.aggrList = aggregate.NewAggregatorList(part.AggrType())

	// TODO: if policy to merge w old chunks need to get prev chunk, vs restart appender

	// issue DB GetItem command to load last state of metric
	path := part.GetMetricPath(metric.name, metric.hash)
	getInput := v3io.GetItemInput{
		Path: path, AttributeNames: []string{"_maxtime"}}

	request, err := mc.container.GetItem(&getInput, metric, mc.responseChan)
	if err != nil {
		mc.logger.ErrorWith("Failed to send GetItem request", "metric", metric.key, "err", err)
		return false, err
	}

	mc.logger.DebugWith("Get Metric State", "name", metric.name, "key", metric.key, "reqid", request.ID)
	return true, nil
}

// Process the GetItem response from the DB and initialize or restore the current chunk
func (cs *chunkStore) processGetResp(mc *MetricsCache, metric *MetricState, resp *v3io.Response) {

	// TODO: init based on schema, use init function, recover old state vs append based on policy
	chunk := chunkenc.NewXORChunk(cs.logger)
	app, _ := chunk.Appender()
	cs.chunks[0].appender = app
	cs.chunks[0].state |= chunkStateFirst

	if resp.Error != nil {
		// assume the item not found   TODO: check error code

		if metric.newName {
			path := mc.cfg.TablePath + "/names/" + metric.name
			putInput := v3io.PutItemInput{Path: path, Attributes: map[string]interface{}{}}

			request, err := mc.container.PutItem(&putInput, metric, mc.nameUpdateChan)
			if err != nil {
				// Count errors
				counter, err := performance.ReporterInstanceFromConfig(mc.cfg).GetCounter("PutNameError")
				if err != nil {
					mc.logger.Error("failed to create performance counter for PutNameError. Error: %v", err)
				} else {
					counter.Inc(1)
				}
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
	cs.lastTid = cs.chunks[0].partition.GetStartTime()

}

// Append data to the right chunk and table based on the time and state
func (cs *chunkStore) Append(t int64, v interface{}) {
	if metricReporter, err := performance.DefaultReporterInstance(); err == nil {
		if counter, err := metricReporter.GetCounter("AppendCounter"); err == nil {
			counter.Inc(1)
		}
	}

	cs.pending = append(cs.pending, pendingData{t: t, v: v})
	// if the new time is older than previous times, sort the list
	if len(cs.pending) > 1 && cs.pending[len(cs.pending)-2].t < t {
		sort.Sort(cs.pending)
	}
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

		chunk := chunkenc.NewXORChunk(cs.logger) // TODO: init based on schema, use init function
		app, err := chunk.Appender()
		if err != nil {
			return nil
		}
		nextPart, _ := part.NextPart(t)
		cur.initialize(nextPart, t)
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
	if prev.partition != nil && prev.inRange(t) && t > cs.maxTime-maxLateArrivalInterval {
		return prev
	}

	return nil
}

// write all pending samples to DB chunks and aggregators
func (cs *chunkStore) writeChunks(mc *MetricsCache, metric *MetricState) (hasPendingUpdates bool, err error) {

	metricReporter := performance.ReporterInstanceFromConfig(mc.cfg)
	writeChunksTimer, err := metricReporter.GetTimer("WriteChunksTimer")
	if err != nil {
		return hasPendingUpdates, errors.Wrap(err, "failed to obtain timer object for [LabelValuesTimer]")
	}

	writeChunksTimer.Time(func() {
		// return if there are no pending updates
		if len(cs.pending) == 0 {
			hasPendingUpdates, err = false, nil
			return
		}

		expr := ""
		notInitialized := false

		// init partition info and find if we need to init the metric headers (labels, ..) in case of new partition
		t0 := cs.pending[0].t
		partition, err := mc.partitionMngr.TimeToPart(t0)
		if err != nil {
			hasPendingUpdates = false
			return
		}
		if partition.GetStartTime() > cs.lastTid {
			notInitialized = true
			cs.lastTid = partition.GetStartTime()
		}

		// init aggregation buckets info
		bucket := partition.Time2Bucket(t0)
		numBuckets := partition.AggrBuckets()
		isNewBucket := bucket > partition.Time2Bucket(cs.maxTime)

		var activeChunk *attrAppender
		var pendingSampleIndex int
		var pendingSamplesCount int

		// loop over pending samples, add to chunks & aggregates (create required update expressions)
		for pendingSampleIndex < len(cs.pending) && pendingSamplesCount < mc.cfg.BatchSize && partition.InRange(cs.pending[pendingSampleIndex].t) {
			sampleTime := cs.pending[pendingSampleIndex].t

			if sampleTime <= cs.initMaxTime && !mc.cfg.OverrideOld {
				mc.logger.DebugWith("Time is less than init max time", "T", sampleTime, "InitMaxTime", cs.initMaxTime)
				pendingSampleIndex++
				continue
			}

			// init activeChunk if nil (when samples are too old), if still too old skip to next sample
			if activeChunk == nil {
				activeChunk = cs.chunkByTime(sampleTime)
				if activeChunk == nil {
					pendingSampleIndex++
					mc.logger.DebugWith("nil active chunk", "T", sampleTime)
					continue
				}
			}

			// advance maximum time processed in metric
			if sampleTime > cs.maxTime {
				cs.maxTime = sampleTime
			}

			// add value to aggregators
			cs.aggrList.Aggregate(sampleTime, cs.pending[pendingSampleIndex].v)

			// add value to compressed raw value chunk
			activeChunk.appendAttr(sampleTime, cs.pending[pendingSampleIndex].v.(float64))

			// if the last item or last item in the same partition add expressions and break
			if (pendingSampleIndex == len(cs.pending)-1) || pendingSamplesCount == mc.cfg.BatchSize-1 || !partition.InRange(cs.pending[pendingSampleIndex+1].t) {
				expr = expr + cs.aggrList.SetOrUpdateExpr("v", bucket, isNewBucket)
				expr = expr + cs.appendExpression(activeChunk)
				pendingSampleIndex++
				pendingSamplesCount++
				break
			}

			// if the next item is in new Aggregate bucket, gen expression and init new bucket
			nextT := cs.pending[pendingSampleIndex+1].t
			nextBucket := partition.Time2Bucket(nextT)
			if nextBucket != bucket {
				expr = expr + cs.aggrList.SetOrUpdateExpr("v", bucket, isNewBucket)
				cs.aggrList.Clear()
				bucket = nextBucket
				isNewBucket = true
			}

			// if the next item is in a new chunk, gen expression and init new chunk
			if !activeChunk.inRange(nextT) {
				expr = expr + cs.appendExpression(activeChunk)
				activeChunk = cs.chunkByTime(nextT)
			}

			pendingSampleIndex++
			pendingSamplesCount++
		}

		cs.aggrList.Clear()
		if pendingSampleIndex == len(cs.pending) {
			cs.pending = cs.pending[:0]
		} else {
			// leave pending not processed or from newer partitions
			cs.pending = cs.pending[pendingSampleIndex:]
		}

		if pendingSamplesCount == 0 || expr == "" {
			if len(cs.pending) > 0 {
				mc.metricQueue.Push(metric)
			}
			hasPendingUpdates, err = false, nil
			return
		}

		// if the table object wasnt initialized, insert init expression
		if notInitialized {
			// init labels (dimension) attributes
			lblexpr := metric.Lset.GetExpr()

			// init aggregate arrays
			lblexpr = lblexpr + cs.aggrList.InitExpr("v", numBuckets)

			expr = lblexpr + fmt.Sprintf("_lset='%s'; ", metric.key) + expr
		}

		// Call V3IO async Update Item method
		expr += fmt.Sprintf("_maxtime=%d;", cs.maxTime) // TODO: use max() expr
		path := partition.GetMetricPath(metric.name, metric.hash)
		request, err := mc.container.UpdateItem(
			&v3io.UpdateItemInput{Path: path, Expression: &expr}, metric, mc.responseChan)
		if err != nil {
			mc.logger.ErrorWith("UpdateItem Failed", "err", err)
			hasPendingUpdates = false
		}

		// add async request ID to the requests map (can be avoided if V3IO will add user data in request)
		mc.logger.DebugWith("updateMetric expression", "name", metric.name, "key", metric.key, "expr", expr, "reqid", request.ID)

		hasPendingUpdates, err = true, nil
		return
	})

	return
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
}

// return the chunk update expression
func (cs *chunkStore) appendExpression(chunk *attrAppender) string {

	if chunk != nil {
		bytes := chunk.appender.Chunk().Bytes()
		chunk.state |= chunkStateWriting

		expr := ""
		idx, err := chunk.partition.TimeToChunkId(chunk.chunkMint)
		if err != nil {
			return ""
		}
		attr := chunk.partition.ChunkID2Attr("v", idx)

		val := base64.StdEncoding.EncodeToString(bytes)

		// overwrite, merge, or append based on the chunk state
		if chunk.state&chunkStateCommitted != 0 || chunk.state&chunkStateMerge != 0 {
			expr = fmt.Sprintf("%s=if_not_exists(%s,blob('')) + blob('%s'); ", attr, attr, val)
		} else {
			expr = fmt.Sprintf("%s=blob('%s'); ", attr, val)
		}

		return expr

	}

	return ""
}
