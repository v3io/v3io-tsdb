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
	"net/http"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-go/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

// Start event loops for handling metric updates (appends and Get/Update DB responses)
// TODO: we can use multiple Go routines and spread the metrics across based on Hash LSB.
func (mc *MetricsCache) start() error {

	mc.nameUpdateRespLoop()
	mc.metricsUpdateLoop(0)
	mc.metricFeed(0)

	return nil
}

// Read data from the append queue, push it into per-metric queues, and manage ingestion states
func (mc *MetricsCache) metricFeed(index int) {

	go func() {
		potentialCompletion := false
		var completeChan chan int

		for {
			select {
			case <-mc.stopChan:
				return
			case app := <-mc.asyncAppendChan:
				newMetrics := 0
				dataQueued := 0
				numPushed := 0
				gotCompletion := false
			inLoop:
				for i := 0; i <= mc.cfg.BatchSize; i++ {
					// Handle completion notifications from the update loop
					if app.isCompletion {
						gotCompletion = true
					} else if app.metric == nil {
						// Handle update completion requests (metric == nil)
						completeChan = app.resp
						if potentialCompletion {
							completeChan <- 0
						}
					} else {
						potentialCompletion = false
						// Handle append requests (Add / AddFast)
						metric := app.metric
						metric.Lock()
						metric.store.numNotProcessed--
						metric.store.Append(app.t, app.v)
						numPushed++
						dataQueued += metric.store.samplesQueueLength()

						// If there are no in-flight requests, add the metric to the queue and update state
						if metric.isReady() || metric.getState() == storeStateInit {

							if metric.getState() == storeStateInit {
								metric.setState(storeStatePreGet)
							}
							if metric.isReady() {
								metric.setState(storeStateAboutToUpdate)
							}

							length := mc.metricQueue.Push(metric)
							if length < 2*mc.cfg.Workers {
								newMetrics++
							}
						}
						metric.Unlock()
					}
					// Poll if we have more updates (accelerate the outer select)
					if i < mc.cfg.BatchSize {
						select {
						case app = <-mc.asyncAppendChan:
						default:
							break inLoop
						}
					}
				}
				// Notify the update loop that there are new metrics to process
				if newMetrics > 0 {
					atomic.AddInt64(&mc.outstandingUpdates, 1)
					mc.newUpdates <- newMetrics
				} else if gotCompletion {
					inFlight := atomic.LoadInt64(&mc.requestsInFlight)
					outstanding := atomic.LoadInt64(&mc.outstandingUpdates)
					if outstanding == 0 && inFlight == 0 {
						switch len(mc.asyncAppendChan) {
						case 0:
							potentialCompletion = true
							if completeChan != nil {
								completeChan <- 0
							}
						case 1:
							potentialCompletion = true
						}
					}
				}
				// If we have too much work, stall the queue for some time
				if numPushed > mc.cfg.BatchSize/2 && dataQueued/numPushed > 64 {
					switch {
					case dataQueued/numPushed <= 96:
						time.Sleep(queueStallTime)
					case dataQueued/numPushed > 96 && dataQueued/numPushed < 200:
						time.Sleep(4 * queueStallTime)
					default:
						time.Sleep(10 * queueStallTime)
					}
				}
			}
		}
	}()
}

// An async loop that accepts new metric updates or responses from previous updates and makes new storage requests
func (mc *MetricsCache) metricsUpdateLoop(index int) {

	go func() {
		counter := 0
		for {
			select {
			case <-mc.stopChan:
				return
			case <-mc.newUpdates:
				// Handle new metric notifications (from metricFeed)
				for mc.updatesInFlight < mc.cfg.Workers*2 {
					freeSlots := mc.cfg.Workers*2 - mc.updatesInFlight
					metrics := mc.metricQueue.PopN(freeSlots)
					for _, metric := range metrics {
						mc.postMetricUpdates(metric)
					}
					if len(metrics) < freeSlots {
						break
					}
				}

				outstandingUpdates := atomic.AddInt64(&mc.outstandingUpdates, -1)

				if atomic.LoadInt64(&mc.requestsInFlight) == 0 && outstandingUpdates == 0 {
					mc.logger.Debug("Return to feed after processing newUpdates")
					mc.asyncAppendChan <- &asyncAppend{isCompletion: true}
				}
			case resp := <-mc.responseChan:
				// Handle V3IO async responses
				nonQueued := mc.metricQueue.IsEmpty()

			inLoop:
				for i := 0; i <= mc.cfg.BatchSize; i++ {

					mc.updatesInFlight--
					counter++
					if counter%3000 == 0 {
						mc.logger.Debug("Handle response: inFly %d, Q %d", mc.updatesInFlight, mc.metricQueue.Length())
					}
					metric := resp.Context.(*MetricState)
					mc.handleResponse(metric, resp, nonQueued)

					// Poll if we have more responses (accelerate the outer select)
					if i < mc.cfg.BatchSize {
						select {
						case resp = <-mc.responseChan:
							atomic.AddInt64(&mc.requestsInFlight, -1)
						default:
							break inLoop
						}
					}
				}

				// Post updates if we have queued metrics and the channel has room for more
				for mc.updatesInFlight < mc.cfg.Workers*2 {
					freeSlots := mc.cfg.Workers*2 - mc.updatesInFlight
					metrics := mc.metricQueue.PopN(freeSlots)
					if len(metrics) == 0 {
						break
					}
					for _, metric := range metrics {
						mc.postMetricUpdates(metric)
					}
				}

				requestsInFlight := atomic.AddInt64(&mc.requestsInFlight, -1)

				// Notify the metric feeder when all in-flight tasks are done
				if requestsInFlight == 0 && atomic.LoadInt64(&mc.outstandingUpdates) == 0 {
					mc.logger.Debug("Return to feed after processing responseChan")
					mc.asyncAppendChan <- &asyncAppend{isCompletion: true}
				}
			}
		}
	}()
}

// Send a request with chunk data to the DB
// If in the initial state, read metric metadata from the DB.
func (mc *MetricsCache) postMetricUpdates(metric *MetricState) {

	metric.Lock()
	defer metric.Unlock()
	var sent bool

	// In case we are in pre get state or our data spreads across multiple partitions, get the new state for the current partition
	if metric.getState() == storeStatePreGet ||
		(metric.canSendRequests() && metric.shouldGetState) {
		sent = mc.sendGetMetricState(metric)
		if sent {
			mc.updatesInFlight++
		}
	} else if metric.canSendRequests() {
		sent = mc.writeChunksAndGetState(metric)

		if !sent {
			if metric.store.samplesQueueLength() == 0 {
				metric.setState(storeStateReady)
			} else {
				if mc.metricQueue.length() > 0 {
					atomic.AddInt64(&mc.outstandingUpdates, 1)
					mc.newUpdates <- mc.metricQueue.length()
				}
			}
		}
	}

}

func (mc *MetricsCache) sendGetMetricState(metric *MetricState) bool {
	// If we are already in a get state, discard
	if metric.getState() == storeStateGet {
		return false
	}

	sent, err := metric.store.getChunksState(mc, metric)
	if err != nil {
		// Count errors
		mc.performanceReporter.IncrementCounter("GetChunksStateError", 1)

		mc.logger.ErrorWith("Failed to get item state", "metric", metric.Lset, "err", err)
		setError(mc, metric, err)
	} else {
		metric.setState(storeStateGet)
	}

	return sent
}

func (mc *MetricsCache) writeChunksAndGetState(metric *MetricState) bool {
	sent, err := metric.store.writeChunks(mc, metric)
	if err != nil {
		// Count errors
		mc.performanceReporter.IncrementCounter("WriteChunksError", 1)

		mc.logger.ErrorWith("Submit failed", "metric", metric.Lset, "err", err)
		setError(mc, metric, errors.Wrap(err, "Chunk write submit failed."))
	} else if sent {
		metric.setState(storeStateUpdate)
	} else if metric.shouldGetState {
		// In case we didn't write any data and the metric state needs to be updated, update it straight away
		sent = mc.sendGetMetricState(metric)
	}

	if sent {
		mc.updatesInFlight++
	}
	return sent
}

// Handle DB responses
// If the backlog queue is empty and have data to send, write more chunks to the DB.
func (mc *MetricsCache) handleResponse(metric *MetricState, resp *v3io.Response, canWrite bool) bool {
	defer resp.Release()
	metric.Lock()
	defer metric.Unlock()

	reqInput := resp.Request().Input

	if resp.Error != nil && metric.getState() != storeStateGet {
		req := reqInput.(*v3io.UpdateItemInput)
		mc.logger.DebugWith("I/O failure", "id", resp.ID, "err", resp.Error, "key", metric.key,
			"in-flight", mc.updatesInFlight, "mqueue", mc.metricQueue.Length(),
			"numsamples", metric.store.samplesQueueLength(), "path", req.Path, "update expression", req.Expression)
	} else {
		mc.logger.DebugWith("I/O response", "id", resp.ID, "err", resp.Error, "key", metric.key, "request type",
			reflect.TypeOf(reqInput), "request", reqInput)
	}

	if metric.getState() == storeStateGet {
		// Handle Get response, sync metric state with the DB
		metric.store.processGetResp(mc, metric, resp)

	} else {
		// Handle Update Expression responses
		if resp.Error == nil {
			if !metric.store.isAggr() {
				// Set fields so next write won't include redundant info (bytes, labels, init_array)
				metric.store.ProcessWriteResp()
			}
			metric.retryCount = 0
		} else {
			clear := func() {
				resp.Release()
				metric.store = newChunkStore(mc.logger, metric.Lset.LabelNames(), metric.store.isAggr())
				metric.retryCount = 0
				metric.setState(storeStateInit)
				mc.cacheMetricMap.ResetMetric(metric.hash)
			}

			// Count errors
			mc.performanceReporter.IncrementCounter("ChunkUpdateRetries", 1)

			// Metrics with too many update errors go into Error state
			metric.retryCount++
			if e, hasStatusCode := resp.Error.(v3ioerrors.ErrorWithStatusCode); hasStatusCode && e.StatusCode() != http.StatusServiceUnavailable {
				// If condition was evaluated as false log this and report this error upstream.
				if utils.IsFalseConditionError(resp.Error) {
					req := reqInput.(*v3io.UpdateItemInput)
					// This might happen on attempt to add metric value of wrong type, i.e. float <-> string
					errMsg := fmt.Sprintf("failed to ingest values of incompatible data type into metric %s.", req.Path)
					mc.logger.DebugWith(errMsg)
					setError(mc, metric, errors.New(errMsg))
				} else {
					mc.logger.ErrorWith(fmt.Sprintf("Chunk update failed with status code %d.", e.StatusCode()))
					setError(mc, metric, errors.Wrap(resp.Error, fmt.Sprintf("Chunk update failed due to status code %d.", e.StatusCode())))
				}
				clear()
				return false
			} else if metric.retryCount == maxRetriesOnWrite {
				mc.logger.ErrorWith(fmt.Sprintf("Chunk update failed - exceeded %d retries", maxRetriesOnWrite), "metric", metric.Lset)
				setError(mc, metric, errors.Wrap(resp.Error, fmt.Sprintf("Chunk update failed after %d retries.", maxRetriesOnWrite)))
				clear()

				// Count errors
				mc.performanceReporter.IncrementCounter("ChunkUpdateRetryExceededError", 1)
				return false
			}
		}
	}

	metric.setState(storeStateReady)

	var sent bool

	// In case our data spreads across multiple partitions, get the new state for the current partition
	if metric.shouldGetState {
		sent = mc.sendGetMetricState(metric)
		if sent {
			mc.updatesInFlight++
		}
	} else if canWrite {
		sent = mc.writeChunksAndGetState(metric)
	} else if metric.store.samplesQueueLength() > 0 {
		mc.metricQueue.Push(metric)
		metric.setState(storeStateAboutToUpdate)
	}
	if !sent && metric.store.numNotProcessed == 0 && metric.store.pending.Len() == 0 {
		mc.cacheMetricMap.ResetMetric(metric.hash)
	}

	return sent
}

// Handle responses for names table updates
func (mc *MetricsCache) nameUpdateRespLoop() {

	go func() {
		for {
			select {
			case <-mc.stopChan:
				return
			case resp := <-mc.nameUpdateChan:
				// Handle V3IO PutItem in names table
				metric, ok := resp.Context.(*MetricState)
				if ok {
					metric.Lock()
					if resp.Error != nil {
						// Count errors
						mc.performanceReporter.IncrementCounter("UpdateNameError", 1)

						mc.logger.ErrorWith("Update-name process failed", "id", resp.ID, "name", metric.name)
					} else {
						mc.logger.DebugWith("Update-name process response", "id", resp.ID, "name", metric.name)
					}
					metric.Unlock()
				}

				resp.Release()

				requestsInFlight := atomic.AddInt64(&mc.requestsInFlight, -1)

				if requestsInFlight == 0 && atomic.LoadInt64(&mc.outstandingUpdates) == 0 {
					mc.logger.Debug("Return to feed after processing nameUpdateChan")
					mc.asyncAppendChan <- &asyncAppend{isCompletion: true}
				}
			}
		}
	}()
}

func setError(mc *MetricsCache, metric *MetricState, err error) {
	metric.setError(err)
	mc.lastError = err
}
