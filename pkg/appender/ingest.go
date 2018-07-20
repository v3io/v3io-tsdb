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
				length := mc.extraQueue.Length()
				mc.logger.Debug("Complete Update cycle - inflight %d len %d\n", inFlight, length)

				if length == 0 && gotData {
					gotCompletion = true
				}

				if completeChan != nil && length == 0 && gotData {
					completeChan <- 0
					gotData = false
				}

			case app := <-mc.asyncAppendChan:
				newMetrics := 0
				maxPending := 0
			inLoop:
				for i := 0; i < maxSampleLoop; i++ {

					if app.metric == nil {
						// handle Completion Notification requests (metric == nil)
						completeChan = app.resp

						length := mc.extraQueue.Length()
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

// async loop which accept new metric updates or responses from previous updates and make new storage requests
func (mc *MetricsCache) metricsUpdateLoop(index int) {

	go func() {
		counter := 0

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
				length := mc.extraQueue.Length()
				counter++
				metric := resp.Context.(*MetricState)
				mc.handleResponse(metric, resp, length == 0, counter)

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

func (mc *MetricsCache) postMetricUpdates(metric *MetricState) {

	metric.Lock()
	defer metric.Unlock()
	sent := false
	var err error
	//fmt.Println("NEW Post ", metric.Lset, mc.updatesInFlight, len(metric.store.pending), metric.store.GetState())

	if metric.store.GetState() == storeStatePreGet {
		sent, err = metric.store.GetChunksState(mc, metric, metric.store.pending[0].t)
		if err != nil {
			metric.err = err
		}

	} else {
		sent, err = metric.store.WriteChunks(mc, metric)
		if err != nil {
			mc.logger.ErrorWith("Submit failed", "metric", metric.Lset, "err", err)
			metric.err = errors.Wrap(err, "chunk write submit failed")
			metric.store.SetState(storeStateError)
		}
	}

	if sent {
		mc.updatesInFlight++
	}
}

func (mc *MetricsCache) handleResponse(metric *MetricState, resp *v3io.Response, canWrite bool, counter int) bool {
	metric.Lock()
	defer metric.Unlock()

	if resp.Error != nil && metric.store.GetState() != storeStateGet {
		mc.logger.ErrorWith("failed IO", "id", resp.ID, "err", resp.Error, "key", metric.key)
	} else {
		mc.logger.DebugWith("IO resp", "id", resp.ID, "err", resp.Error, "key", metric.key)
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

	//length := mc.extraQueue.Length()
	if counter%200 == 0 {
		fmt.Println("RESP ", metric.Lset, mc.updatesInFlight, len(metric.store.pending), canWrite)
	}
	var sent bool
	var err error

	if canWrite {
		sent, err = metric.store.WriteChunks(mc, metric)
		if err != nil {
			mc.logger.ErrorWith("Submit failed", "metric", metric.Lset, "err", err)
			metric.err = errors.Wrap(err, "chunk write submit failed")
			metric.store.SetState(storeStateError)
		}
		if sent {
			mc.updatesInFlight++
		}

	} else if len(metric.store.pending) > 0 {
		metric.store.state = storeStateUpdate
		mc.extraQueue.Push(metric)
	}

	return sent
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
