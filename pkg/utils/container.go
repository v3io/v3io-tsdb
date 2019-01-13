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
	"time"

	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/config"
)

const defaultHttpTimeout = 30 * time.Second

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

func CreateContainer(logger logger.Logger, cfg *config.V3ioConfig) (*v3io.Container, error) {
	// Create context
	context, err := v3io.NewContext(logger, cfg.WebApiEndpoint, cfg.Workers)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a V3IO TSDB client.")
	}

	if cfg.HttpTimeout == "" {
		context.Sync.Timeout = defaultHttpTimeout
	} else {
		timeout, err := time.ParseDuration(cfg.HttpTimeout)
		if err != nil {
			logger.Warn("Failed to parse httpTimeout '%s'. Defaulting to %d millis.", cfg.HttpTimeout, defaultHttpTimeout/time.Millisecond)
			context.Sync.Timeout = defaultHttpTimeout
		} else {
			context.Sync.Timeout = timeout
		}
	}

	// Create session
	sessionConfig := &v3io.SessionConfig{
		Username:   cfg.Username,
		Password:   cfg.Password,
		Label:      "tsdb",
		SessionKey: cfg.AccessKey,
	}
	session, err := context.NewSessionFromConfig(sessionConfig)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a session.")
	}

	// Create the container
	container, err := session.NewContainer(cfg.Container)
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
	input := v3io.GetItemsInput{Path: path, AttributeNames: []string{config.ObjectNameAttrName}, Filter: filter}
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
		name := iter.GetField(config.ObjectNameAttrName).(string)
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
