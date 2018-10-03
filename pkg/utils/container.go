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

package utils

import (
	"encoding/binary"
	"fmt"
	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"time"
)

func NewLogger(level string) (logger.Logger, error) {
	var logLevel nucliozap.Level
	switch level {
	case "debug":
		logLevel = nucliozap.DebugLevel
	case "info":
		logLevel = nucliozap.InfoLevel
	case "warn":
		logLevel = nucliozap.WarnLevel
	case "error":
		logLevel = nucliozap.ErrorLevel
	default:
		logLevel = nucliozap.WarnLevel
	}

	log, err := nucliozap.NewNuclioZapCmd("v3io-prom", logLevel)
	if err != nil {
		return nil, err
	}
	return log, nil
}

func CreateContainer(logger logger.Logger, addr, cont, username, password string, workers int) (*v3io.Container, error) {
	// Create context
	context, err := v3io.NewContext(logger, addr, workers)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a V3IO TSDB client.")
	}

	// Create session
	session, err := context.NewSession(username, password, "v3test")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a session.")
	}

	// Create the container
	container, err := session.NewContainer(cont)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a container.")
	}

	return container, nil
}

// Convert a V3IO blob to an integers array
func AsInt64Array(val []byte) []uint64 {
	var array []uint64
	bytes := val
	for i := 16; i+8 <= len(bytes); i += 8 {
		val := binary.LittleEndian.Uint64(bytes[i : i+8])
		array = append(array, val)
	}
	return array
}

func DeleteTable(logger logger.Logger, container *v3io.Container, path, filter string, workers int) error {
	input := v3io.GetItemsInput{Path: path, AttributeNames: []string{"__name"}, Filter: filter}
	iter, err := NewAsyncItemsCursor(container, &input, workers, []string{}, logger)
	if err != nil {
		return err
	}

	responseChan := make(chan *v3io.Response, 1000)
	commChan := make(chan int, 2)
	doneChan := respWaitLoop(commChan, responseChan, 10*time.Second)
	reqMap := map[uint64]bool{}

	i := 0
	for iter.Next() {
		name := iter.GetField("__name").(string)
		req, err := container.DeleteObject(&v3io.DeleteObjectInput{Path: path + "/" + name}, nil, responseChan)
		if err != nil {
			commChan <- i
			return errors.Wrapf(err, "Failed to delete object '%s'.", name)
		}
		reqMap[req.ID] = true
		i++
	}

	commChan <- i
	if iter.Err() != nil {
		return errors.Wrap(iter.Err(), "Failed to delete object.")
	}

	<-doneChan

	return nil
}

func respWaitLoop(comm chan int, responseChan chan *v3io.Response, timeout time.Duration) chan bool {
	responses := 0
	requests := -1
	done := make(chan bool)

	go func() {
		active := false
		for {
			select {

			case resp := <-responseChan:
				responses++
				active = true

				if resp.Error != nil {
					fmt.Println(resp.Error, "Failed to receive a response to delete request.")
				}

				if requests == responses {
					done <- true
					return
				}

			case requests = <-comm:
				if requests <= responses {
					done <- true
					return
				}

			case <-time.After(timeout):
				if !active {
					fmt.Println("\nResponse loop timed out.", requests, responses)
					done <- true
					return
				} else {
					active = false
				}
			}
		}
	}()

	return done
}
