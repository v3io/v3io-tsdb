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
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"sort"
)

// TODO: make it configurable
const maxLateArrivalInterval = 59 * 60 * 1000 // Max late arrival of 59min

// Create a chunk store with two chunks (current, previous)
func NewChunkStore(logger logger.Logger) *chunkStore {
	store := chunkStore{logger: logger}
	store.chunks[0] = &attrAppender{}
	store.chunks[1] = &attrAppender{}
	store.performanceReporter, _ = performance.DefaultReporterInstance()

	return &store
}

// chunkStore store state & latest + previous chunk appenders
type chunkStore struct {
	logger              logger.Logger
	performanceReporter *performance.MetricReporter

	curChunk int
	lastTid  int64
	chunks   [2]*attrAppender

	aggrList      *aggregate.AggregatesList
	pending       pendingList
	maxTime       int64
	initMaxTime   int64 // Max time read from DB metric before first append
	delRawSamples bool  // TODO: for metrics w aggregates only
}

func (cs *chunkStore) samplesQueueLength() int {
	return len(cs.pending)
}

// Chunk appender object, state used for appending t/v to a chunk
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

// Initialize/clear the chunk appender
func (a *attrAppender) initialize(partition *partmgr.DBPartition, t int64) {
	a.state = 0
	a.partition = partition
	a.chunkMint = partition.GetChunkMint(t)
}

// Check whether the specified time (t) is within the chunk range
func (a *attrAppender) inRange(t int64) bool {
	return a.partition.InChunkRange(a.chunkMint, t)
}

// Check whether the specified time (t) is ahead of the chunk range
func (a *attrAppender) isAhead(t int64) bool {
	return a.partition.IsAheadOfChunk(a.chunkMint, t)
}

// Append a single t/v pair to a chunk
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

// Read (async) the current chunk state and data from the storage, used in the first chunk access
func (cs *chunkStore) getChunksState(mc *MetricsCache, metric *MetricState) (bool, error) {

	if len(cs.pending) == 0 {
		return false, nil
	}
	// Init chunk and create an aggregates-list object based on the partition policy
	t := cs.pending[0].t
	part, err := mc.partitionMngr.TimeToPart(t)
	if err != nil {
		return false, err
	}
	cs.chunks[0].initialize(part, t)
	cs.aggrList = aggregate.NewAggregatesList(part.AggrType())

	// TODO: if policy to merge w old chunks needs to get prev chunk, vs restart appender

	// Issue a GetItem command to the DB to load last state of metric
	path := part.GetMetricPath(metric.name, metric.hash)
	getInput := v3io.GetItemInput{
		Path: path, AttributeNames: []string{"_maxtime"}}

	request, err := mc.container.GetItem(&getInput, metric, mc.responseChan)
	if err != nil {
		mc.logger.ErrorWith("Failed to send a GetItem request to the TSDB", "metric", metric.key, "err", err)
		return false, err
	}

	mc.logger.DebugWith("Get metric state", "name", metric.name, "key", metric.key, "reqid", request.ID)
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
				cs.performanceReporter.IncrementCounter("PutNameError", 1)

				mc.logger.ErrorWith("Update-name PutItem failed", "metric", metric.key, "err", err)
			} else {
				mc.logger.DebugWith("Update name", "name", metric.name, "key", metric.key, "reqid", request.ID)
			}
		}

		return
	}

	// Check and update the metric item's end time (maxt) timestamp, allow continuing from the last point in case of failure
	item := resp.Output.(*v3io.GetItemOutput).Item
	var maxTime int64
	val := item["_maxtime"]
	if val != nil {
		maxTime = int64(val.(int))
	}
	mc.logger.DebugWith("Got metric item", "name", metric.name, "key", metric.key, "maxt", maxTime)

	if !mc.cfg.OverrideOld {
		cs.maxTime = maxTime
		cs.initMaxTime = maxTime
	}

	if cs.chunks[0].inRange(maxTime) && !mc.cfg.OverrideOld {
		cs.chunks[0].state |= chunkStateMerge
	}

	// Set Last TableId - indicate that there is no need to create metric object
	cs.lastTid = cs.chunks[0].partition.GetStartTime()

}

// Append data to the right chunk and table based on the time and state
func (cs *chunkStore) Append(t int64, v interface{}) {
	if metricReporter, err := performance.DefaultReporterInstance(); err == nil {
		metricReporter.IncrementCounter("AppendCounter", 1)
	}

	cs.pending = append(cs.pending, pendingData{t: t, v: v})
	// If the new time is older than previous times, sort the list
	if len(cs.pending) > 1 && cs.pending[len(cs.pending)-2].t < t {
		sort.Sort(cs.pending)
	}
}

// Return current, previous, or create new  chunk based on sample time
func (cs *chunkStore) chunkByTime(t int64) *attrAppender {

	// Sample is in the current chunk
	cur := cs.chunks[cs.curChunk]
	if cur.inRange(t) {
		return cur
	}

	// Sample is in the next chunk, need to initialize
	if cur.isAhead(t) {
		// Time is ahead of this chunk time, advance the current chunk
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

	// If it's the first chunk after init we don't allow old updates
	if (cur.state & chunkStateFirst) != 0 {
		return nil
	}

	prev := cs.chunks[cs.curChunk^1]
	// Delayed appends - only allowed to previous chunk or within allowed window
	if prev.partition != nil && prev.inRange(t) && t > cs.maxTime-maxLateArrivalInterval {
		return prev
	}

	return nil
}

// Write all pending samples to DB chunks and aggregates
func (cs *chunkStore) writeChunks(mc *MetricsCache, metric *MetricState) (hasPendingUpdates bool, err error) {
	cs.performanceReporter.WithTimer("WriteChunksTimer", func() {
		// Return if there are no pending updates
		if len(cs.pending) == 0 {
			hasPendingUpdates, err = false, nil
			return
		}

		expr := ""
		notInitialized := false

		// Init the partition info and find whether we need to init the metric headers (labels, ..) in the case of a new partition
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

		// Init the aggregation-buckets info
		bucket := partition.Time2Bucket(t0)
		numBuckets := partition.AggrBuckets()
		isNewBucket := bucket > partition.Time2Bucket(cs.maxTime)

		var activeChunk *attrAppender
		var pendingSampleIndex int
		var pendingSamplesCount int

		// Loop over pending samples, add to chunks & aggregates (create required update expressions)
		for pendingSampleIndex < len(cs.pending) && pendingSamplesCount < mc.cfg.BatchSize && partition.InRange(cs.pending[pendingSampleIndex].t) {
			sampleTime := cs.pending[pendingSampleIndex].t

			if sampleTime <= cs.initMaxTime && !mc.cfg.OverrideOld {
				mc.logger.WarnWith("Omitting the sample - time is earlier than the last sample time for this metric", "metric", metric.Lset, "T", sampleTime, "InitMaxTime", cs.initMaxTime)
				pendingSampleIndex++
				continue
			}

			// Init activeChunk if nil (when samples are too old); if still too
			// old, skip to next sample
			if activeChunk == nil {
				activeChunk = cs.chunkByTime(sampleTime)
				if activeChunk == nil {
					pendingSampleIndex++
					mc.logger.DebugWith("nil active chunk", "T", sampleTime)
					continue
				}
			}

			// Advance maximum time processed in metric
			if sampleTime > cs.maxTime {
				cs.maxTime = sampleTime
			}

			// Add a value to the aggregates list
			cs.aggrList.Aggregate(sampleTime, cs.pending[pendingSampleIndex].v)

			// Add a value to the compressed raw-values chunk
			activeChunk.appendAttr(sampleTime, cs.pending[pendingSampleIndex].v.(float64))

			// If this is the last item or last item in the same partition, add
			// expressions and break
			if (pendingSampleIndex == len(cs.pending)-1) || pendingSamplesCount == mc.cfg.BatchSize-1 || !partition.InRange(cs.pending[pendingSampleIndex+1].t) {
				expr = expr + cs.aggrList.SetOrUpdateExpr("v", bucket, isNewBucket)
				expr = expr + cs.appendExpression(activeChunk)
				pendingSampleIndex++
				pendingSamplesCount++
				break
			}

			// If the next item is in new Aggregate bucket, generate an
			// expression and initialize the new bucket
			nextT := cs.pending[pendingSampleIndex+1].t
			nextBucket := partition.Time2Bucket(nextT)
			if nextBucket != bucket {
				expr = expr + cs.aggrList.SetOrUpdateExpr("v", bucket, isNewBucket)
				cs.aggrList.Clear()
				bucket = nextBucket
				isNewBucket = true
			}

			// If the next item is in a new chunk, generate an expression and
			// initialize the new chunk
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
			// Leave pending unprocessed or from newer partitions
			cs.pending = cs.pending[pendingSampleIndex:]
		}

		if pendingSamplesCount == 0 || expr == "" {
			if len(cs.pending) > 0 {
				mc.metricQueue.Push(metric)
			}
			hasPendingUpdates, err = false, nil
			return
		}

		// If the table object wasn't initialized, insert an init expression
		if notInitialized {
			// Initialize label (dimension) attributes
			lblexpr := metric.Lset.GetExpr()

			// Initialize aggregate arrays
			lblexpr = lblexpr + cs.aggrList.InitExpr("v", numBuckets)

			expr = lblexpr + fmt.Sprintf("_lset='%s'; ", metric.key) + expr
		}

		// Call the V3IO async UpdateItem method
		expr += fmt.Sprintf("_maxtime=%d;", cs.maxTime) // TODO: use max() expr
		path := partition.GetMetricPath(metric.name, metric.hash)
		request, err := mc.container.UpdateItem(
			&v3io.UpdateItemInput{Path: path, Expression: &expr}, metric, mc.responseChan)
		if err != nil {
			mc.logger.ErrorWith("UpdateItem failed", "err", err)
			hasPendingUpdates = false
		}

		// Add the async request ID to the requests map (can be avoided if V3IO
		// will add user data in request)
		mc.logger.DebugWith("Update-metric expression", "name", metric.name, "key", metric.key, "expr", expr, "reqid", request.ID)

		hasPendingUpdates, err = true, nil
		cs.performanceReporter.UpdateHistogram("WriteChunksSizeHistogram", int64(pendingSamplesCount))
		return
	})

	return
}

// Process the (async) response for the chunk update request
func (cs *chunkStore) ProcessWriteResp() {

	for _, chunk := range cs.chunks {
		// Update the chunk state (if it was written to)
		if chunk.state&chunkStateWriting != 0 {
			chunk.state |= chunkStateCommitted
			chunk.state &^= chunkStateWriting
			chunk.appender.Chunk().Clear()
		}
	}
}

// Return the chunk's update expression
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

		// Overwrite, merge, or append based on the chunk state
		if chunk.state&chunkStateCommitted != 0 || chunk.state&chunkStateMerge != 0 {
			expr = fmt.Sprintf("%s=if_not_exists(%s,blob('')) + blob('%s'); ", attr, attr, val)
		} else {
			expr = fmt.Sprintf("%s=blob('%s'); ", attr, val)
		}

		return expr

	}

	return ""
}
