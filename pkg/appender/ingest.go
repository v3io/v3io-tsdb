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
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"time"
)

// start event loops for handling metric updates (appends and Get/Update DB responses)
// TODO: we can use multiple Go routines and spread the metrics across based on Hash LSB
func (mc *MetricsCache) start() error {

	mc.nameUpdateRespLoop()
	mc.metricsUpdateLoop(0)
	mc.metricFeed(0)

	return nil
}

// Reads data from append queue, push into per metric queues, and manage ingestion states
func (mc *MetricsCache) metricFeed(index int) {

	go func() {
		inFlight := 0
		gotData := false
		gotCompletion := false
		var completeChan chan int

		for {

			select {

			case inFlight = <-mc.updatesComplete:
				// handle completion notifications from update loop
				length := mc.metricQueue.Length()
				mc.logger.Info("Complete Update cycle - inflight %d len %d\n", inFlight, length)

				// if data was sent and the queue is empty mark as completion
				if length == 0 && gotData {
					gotCompletion = true
					if completeChan != nil {
						completeChan <- 0
						gotData = false
					}
				}

			case app := <-mc.asyncAppendChan:
				newMetrics := 0
				queuedEnough := false

			inLoop:
				for i := 0; i <= maxSampleLoop; i++ {

					if app.metric == nil {
						// handle Completion update requests (metric == nil)
						completeChan = app.resp

						length := mc.metricQueue.Length()
						if gotCompletion && length == 0 {
							completeChan <- 0
							gotCompletion = false
							gotData = false
						}

					} else {
						// Handle append requests (Add / AddFast)
						gotData = true
						metric := app.metric
						metric.Lock()

						if !metric.HasError() {

							metric.store.Append(app.t, app.v)
							queuedEnough = queuedEnough || metric.store.QueuedEnough()

							// if there are no in flight requests, add the metric to the queue and update state
							if metric.IsReady() || metric.GetState() == storeStateInit {

								if metric.GetState() == storeStateInit {
									metric.SetState(storeStatePreGet)
								}
								if metric.IsReady() {
									metric.SetState(storeStateUpdate)
								}

								length := mc.metricQueue.Push(metric)
								if length < 2*mc.cfg.Workers {
									newMetrics++
								}
							}

						}
						metric.Unlock()
					}

					// poll if we have more updates (accelerate the outer select)
					if i < maxSampleLoop {
						select {
						case app = <-mc.asyncAppendChan:
						default:
							break inLoop
						}
					}
				}
				// notify update loop there are new metrics to process
				if newMetrics > 0 {
					mc.newUpdates <- newMetrics
				}

				// If we have too much work, stall the queue for some time
				if queuedEnough {
					time.Sleep(queueStallTime)
				}
			}
		}
	}()
}

// async loop which accept new metric updates or responses from previous updates and make new storage requests
func (mc *MetricsCache) metricsUpdateLoop(index int) {

	go func() {
		counter := 0
		for {

			select {

			case newMetrics := <-mc.newUpdates:
				// Handle new metric notifications (from metricFeed)
				freeSlots := mc.cfg.Workers*2 - mc.updatesInFlight
				if newMetrics > freeSlots {
					newMetrics = freeSlots
				}

				for _, metric := range mc.metricQueue.PopN(newMetrics) {
					mc.postMetricUpdates(metric)
				}

			case resp := <-mc.responseChan:
				// Handle V3io async responses
				nonQueued := mc.metricQueue.IsEmpty()

			inLoop:
				for i := 0; i <= maxSampleLoop; i++ {

					mc.updatesInFlight--
					counter++
					if counter%1000 == 0 {
						mc.logger.Info("Handle resp: inFly %d, Q %d", mc.updatesInFlight, mc.metricQueue.Length())
					}
					metric := resp.Context.(*MetricState)
					mc.handleResponse(metric, resp, nonQueued)

					// poll if we have more responses (accelerate the outer select)
					if i < maxSampleLoop {
						select {
						case resp = <-mc.responseChan:
						default:
							break inLoop
						}
					}
				}

				// Post updates if we have queued metrics and the channel has room for more
				freeSlots := mc.cfg.Workers*2 - mc.updatesInFlight
				for _, metric := range mc.metricQueue.PopN(freeSlots) {
					mc.postMetricUpdates(metric)
				}

				// Notify the metric feeder when all in-flight tasks are done
				if mc.updatesInFlight == 0 {
					mc.updatesComplete <- 0
				}
			}
		}
	}()
}

// send request with chunk data to the DB, if in initial state read metric metadata from DB
func (mc *MetricsCache) postMetricUpdates(metric *MetricState) {

	metric.Lock()
	defer metric.Unlock()
	sent := false
	var err error
	//fmt.Println("NEW Post ", metric.Lset, mc.updatesInFlight, metric.store.QueuedSamples(), metric.store.GetState())

	if metric.GetState() == storeStatePreGet {
		sent, err = metric.store.GetChunksState(mc, metric)
		if err != nil {
			metric.SetError(err)
		} else {
			metric.SetState(storeStateGet)
		}

	} else {
		sent, err = metric.store.WriteChunks(mc, metric)
		if err != nil {
			mc.logger.ErrorWith("Submit failed", "metric", metric.Lset, "err", err)
			metric.SetError(errors.Wrap(err, "chunk write submit failed"))
		} else if sent {
			metric.SetState(storeStateUpdate)
		}
	}

	if sent {
		mc.updatesInFlight++
	}
}

// handle DB responses, if the backlog queue is empty and have data to send write more chunks to DB
func (mc *MetricsCache) handleResponse(metric *MetricState, resp *v3io.Response, canWrite bool) bool {
	metric.Lock()
	defer metric.Unlock()

	if resp.Error != nil && metric.GetState() != storeStateGet {
		mc.logger.ErrorWith("failed IO", "id", resp.ID, "err", resp.Error, "key", metric.key,
			"inflight", mc.updatesInFlight, "mqueue", mc.metricQueue.Length(), "numsamples", metric.store.NumQueuedSamples())
	} else {
		mc.logger.DebugWith("IO resp", "id", resp.ID, "err", resp.Error, "key", metric.key)
	}

	if metric.GetState() == storeStateGet {
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
				metric.SetError(errors.Wrap(resp.Error, "chunk update failed after few retries"))
				return false
			}
		}
	}

	resp.Release()
	metric.SetState(storeStateReady)

	var sent bool
	var err error

	if canWrite {
		sent, err = metric.store.WriteChunks(mc, metric)
		if err != nil {
			mc.logger.ErrorWith("Submit failed", "metric", metric.Lset, "err", err)
			metric.SetError(errors.Wrap(err, "chunk write submit failed"))
		} else if sent {
			metric.SetState(storeStateUpdate)
			mc.updatesInFlight++
		}

	} else if metric.store.NumQueuedSamples() > 0 {
		mc.metricQueue.Push(metric)
		metric.SetState(storeStateUpdate)
	}

	return sent
}

// handle responses for names table updates
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
