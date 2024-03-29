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
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/nuclio/logger"
	"github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

// TODO: make it configurable
const maxLateArrivalInterval = 59 * 60 * 1000 // Max late arrival of 59min

// Create a chunk store with two chunks (current, previous)
func newChunkStore(logger logger.Logger, labelNames []string, aggrsOnly bool) *chunkStore {
	store := chunkStore{
		logger:  logger,
		lastTid: -1,
	}
	if !aggrsOnly {
		store.chunks[0] = &attrAppender{}
		store.chunks[1] = &attrAppender{}
	}
	store.labelNames = labelNames
	store.performanceReporter, _ = performance.DefaultReporterInstance()
	return &store
}

// chunkStore store state & latest + previous chunk appenders
type chunkStore struct {
	logger              logger.Logger
	performanceReporter *performance.MetricReporter

	curChunk int
	nextTid  int64
	lastTid  int64
	chunks   [2]*attrAppender

	labelNames      []string
	aggrList        *aggregate.AggregatesList
	pending         pendingList
	maxTime         int64
	delRawSamples   bool // TODO: for metrics w aggregates only
	numNotProcessed int64
}

func (cs *chunkStore) isAggr() bool {
	return cs.chunks[0] == nil
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
	// chunkStateMerge     chunkState = 2 - Deprecated
	// chunkStateCommitted chunkState = 4 - Deprecated
	chunkStateFirst   chunkState = 1
	chunkStateWriting chunkState = 8
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
func (a *attrAppender) appendAttr(t int64, v interface{}) {
	a.appender.Append(t, v)
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
	if !cs.isAggr() {
		cs.chunks[0].initialize(part, t)
	}
	cs.nextTid = t
	cs.aggrList = aggregate.NewAggregatesList(part.AggrType())

	// TODO: if policy to merge w old chunks needs to get prev chunk, vs restart appender

	// Issue a GetItem command to the DB to load last state of metric
	path := part.GetMetricPath(metric.name, metric.hash, cs.labelNames, cs.isAggr())
	getInput := v3io.GetItemInput{
		Path: path, AttributeNames: []string{config.MaxTimeAttrName}}

	atomic.AddInt64(&mc.requestsInFlight, 1)
	request, err := mc.container.GetItem(&getInput, metric, mc.responseChan)
	if err != nil {
		atomic.AddInt64(&mc.requestsInFlight, -1)
		mc.logger.ErrorWith("Failed to send a GetItem request to the TSDB", "metric", metric.key, "err", err)
		return false, err
	}

	mc.logger.DebugWith("Get metric state", "name", metric.name, "key", metric.key, "reqid", request.ID)
	return true, nil
}

// Process the GetItem response from the DB and initialize or restore the current chunk
func (cs *chunkStore) processGetResp(mc *MetricsCache, metric *MetricState, resp *v3io.Response) {

	if !cs.isAggr() {
		// TODO: init based on schema, use init function, recover old state vs append based on policy
		chunk := chunkenc.NewChunk(cs.logger, metric.isVariant)
		app, _ := chunk.Appender()
		cs.chunks[0].appender = app
		cs.chunks[0].state |= chunkStateFirst
	}

	latencyNano := time.Now().UnixNano() - resp.Request().SendTimeNanoseconds
	cs.performanceReporter.UpdateHistogram("UpdateMetricLatencyHistogram", latencyNano)

	if resp.Error != nil {
		if utils.IsNotExistsError(resp.Error) {
			if !metric.created {
				metric.created = true
				path := filepath.Join(mc.cfg.TablePath, config.NamesDirectory, metric.name)
				putInput := v3io.PutItemInput{Path: path, Attributes: map[string]interface{}{}}

				atomic.AddInt64(&mc.requestsInFlight, 1)
				request, err := mc.container.PutItem(&putInput, metric, mc.nameUpdateChan)
				if err != nil {
					atomic.AddInt64(&mc.requestsInFlight, -1)
					cs.performanceReporter.IncrementCounter("PutNameError", 1)
					mc.logger.ErrorWith("Update-name PutItem failed", "metric", metric.key, "err", err)
				} else {
					mc.logger.DebugWith("Update name", "name", metric.name, "key", metric.key, "reqid", request.ID)
				}
			}
		} else {
			mc.logger.Error("Update metric has failed with error: %v", resp.Error)
			cs.performanceReporter.IncrementCounter("UpdateMetricError", 1)
		}

		metric.shouldGetState = false
		cs.maxTime = 0 // In case of an error, initialize max time back to default
		return
	}

	// Check and update the metric item's end time (maxt) timestamp, allow continuing from the last point in case of failure
	item := resp.Output.(*v3io.GetItemOutput).Item
	var maxTime int64
	val := item[config.MaxTimeAttrName]
	if val != nil {
		maxTime = int64(val.(int))
	}
	mc.logger.DebugWith("Got metric item", "name", metric.name, "key", metric.key, "maxt", maxTime)

	if !mc.cfg.OverrideOld {
		cs.maxTime = maxTime
	}

	// Set Last TableId - indicate that there is no need to create metric object
	cs.lastTid = cs.nextTid
	metric.shouldGetState = false
}

// Append data to the right chunk and table based on the time and state
func (cs *chunkStore) Append(t int64, v interface{}) {
	if metricReporter, err := performance.DefaultReporterInstance(); err == nil {
		metricReporter.IncrementCounter("AppendCounter", 1)
	}

	cs.pending = append(cs.pending, pendingData{t: t, v: v})
	// If the new time is older than previous times, sort the list
	if len(cs.pending) > 1 && cs.pending[len(cs.pending)-2].t > t {
		sort.Sort(cs.pending)
	}
}

// Return current, previous, or create new  chunk based on sample time
func (cs *chunkStore) chunkByTime(t int64, isVariantEncoding bool) (*attrAppender, error) {

	// Sample is in the current chunk
	cur := cs.chunks[cs.curChunk]
	if cur.inRange(t) {
		return cur, nil
	}

	// Sample is in the next chunk, need to initialize
	if cur.isAhead(t) {
		// Time is ahead of this chunk time, advance the current chunk
		part := cur.partition
		cur = cs.chunks[cs.curChunk^1]

		chunk := chunkenc.NewChunk(cs.logger, isVariantEncoding) // TODO: init based on schema, use init function
		app, err := chunk.Appender()
		if err != nil {
			return nil, err
		}
		nextPart, err := part.NextPart(t)
		if err != nil {
			return nil, err
		}
		cur.initialize(nextPart, t)
		cs.nextTid = t
		cur.appender = app
		cs.curChunk = cs.curChunk ^ 1

		return cur, nil
	}

	// If it's the first chunk after init we don't allow old updates
	if (cur.state & chunkStateFirst) != 0 {
		return nil, nil
	}

	prev := cs.chunks[cs.curChunk^1]
	// Delayed appends - only allowed to previous chunk or within allowed window
	if prev.partition != nil && prev.inRange(t) && t > cs.maxTime-maxLateArrivalInterval {
		return prev, nil
	}

	return nil, nil
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
		var partition *partmgr.DBPartition
		partition, err = mc.partitionMngr.TimeToPart(t0)
		if err != nil {
			hasPendingUpdates = false
			return
		}

		// In case the current max time is not up to date, force a get state
		if partition.GetStartTime() > cs.maxTime && cs.maxTime > 0 {
			metric.shouldGetState = true
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

			if sampleTime <= cs.maxTime && !mc.cfg.OverrideOld {
				mc.logger.WarnWith("Omitting the sample - time is earlier than the last sample time for this metric", "metric", metric.Lset, "T", sampleTime)

				// If we have reached the end of the pending events and there are events to update, create an update expression and break from loop,
				// Otherwise, discard the event and continue normally
				if pendingSampleIndex == len(cs.pending)-1 {
					if pendingSamplesCount > 0 {
						expr = expr + cs.aggrList.SetOrUpdateExpr("v", bucket, isNewBucket)
						expr = expr + cs.appendExpression(activeChunk)
					}
					pendingSampleIndex++
					break
				} else {
					pendingSampleIndex++
					continue
				}
			}

			// Init activeChunk if nil (when samples are too old); if still too
			// old, skip to next sample
			if !cs.isAggr() && activeChunk == nil {
				activeChunk, err = cs.chunkByTime(sampleTime, metric.isVariant)
				if err != nil {
					hasPendingUpdates = false
					return
				}
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

			if activeChunk != nil {
				// Add a value to the compressed raw-values chunk
				activeChunk.appendAttr(sampleTime, cs.pending[pendingSampleIndex].v)
			}

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
			if activeChunk != nil && !activeChunk.inRange(nextT) {
				expr = expr + cs.appendExpression(activeChunk)
				activeChunk, err = cs.chunkByTime(nextT, metric.isVariant)
				if err != nil {
					hasPendingUpdates = false
					return
				}
			}

			pendingSampleIndex++
			pendingSamplesCount++
		}

		// In case we advanced to a newer partition mark we need to get state again
		if pendingSampleIndex < len(cs.pending) && !partition.InRange(cs.pending[pendingSampleIndex].t) {
			metric.shouldGetState = true
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
			hasPendingUpdates = false
			return
		}

		// If the table object wasn't initialized, insert an init expression
		if notInitialized {
			// Initialize label (dimension) attributes
			lblexpr := metric.Lset.GetExpr()

			// Initialize aggregate arrays
			lblexpr = lblexpr + cs.aggrList.InitExpr("v", numBuckets)

			var encodingExpr string
			if !cs.isAggr() {
				encodingExpr = fmt.Sprintf("%s='%d'; ", config.EncodingAttrName, activeChunk.appender.Encoding())
			}
			lsetExpr := fmt.Sprintf("%s='%s'; ", config.LabelSetAttrName, metric.key)
			expr = lblexpr + encodingExpr + lsetExpr + expr
		}

		conditionExpr := ""

		// Only add the condition when adding to a data chunk, not when writing data to label pre-aggregated
		if activeChunk != nil {
			// Call the V3IO async UpdateItem method
			conditionExpr = fmt.Sprintf("NOT exists(%s) OR (exists(%s) AND %s == '%d')",
				config.EncodingAttrName, config.EncodingAttrName,
				config.EncodingAttrName, activeChunk.appender.Encoding())
		}
		expr += fmt.Sprintf("%v=%d;", config.MaxTimeAttrName, cs.maxTime) // TODO: use max() expr
		path := partition.GetMetricPath(metric.name, metric.hash, cs.labelNames, cs.isAggr())
		atomic.AddInt64(&mc.requestsInFlight, 1)
		request, err := mc.container.UpdateItem(
			&v3io.UpdateItemInput{Path: path, Expression: &expr, Condition: conditionExpr}, metric, mc.responseChan)
		if err != nil {
			atomic.AddInt64(&mc.requestsInFlight, -1)
			mc.logger.ErrorWith("UpdateItem failed", "err", err)
			hasPendingUpdates = false
		}

		// Add the async request ID to the requests map (can be avoided if V3IO
		// will add user data in request)
		mc.logger.DebugWith("Update-metric expression", "name", metric.name, "key", metric.key, "expr", expr, "reqid", request.ID)

		hasPendingUpdates = true
		cs.performanceReporter.UpdateHistogram("WriteChunksSizeHistogram", int64(pendingSamplesCount))
	})

	return
}

// Process the (async) response for the chunk update request
func (cs *chunkStore) ProcessWriteResp() {

	for _, chunk := range cs.chunks {
		// Update the chunk state (if it was written to)
		if chunk.state&chunkStateWriting != 0 {
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
		idx, err := chunk.partition.TimeToChunkID(chunk.chunkMint)
		if err != nil {
			return ""
		}
		attr := chunk.partition.ChunkID2Attr("v", idx)

		val := base64.StdEncoding.EncodeToString(bytes)

		expr = fmt.Sprintf("%s=if_not_exists(%s,blob('')) + blob('%s'); ", attr, attr, val)

		return expr

	}

	return ""
}
