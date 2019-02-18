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

package v3ioutils

import (
	"encoding/binary"
	"net/url"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"

	"github.com/v3io/frames"
	v3io "github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
)

const v3ioUsersContainer = "users"
const v3ioHomeVar = "$V3IO_HOME"

func NewContainer(session *frames.Session, logger logger.Logger, workers int) (*v3io.Container, error) {

	config := v3io.SessionConfig{
		Username:   session.User,
		Password:   session.Password,
		Label:      "v3frames",
		SessionKey: session.Token}

	container, err := createContainer(
		logger, session.Url, session.Container, &config, workers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create data container")
	}

	return container, nil

}

// CreateContainer creates a new container
func createContainer(logger logger.Logger, addr, cont string, config *v3io.SessionConfig, workers int) (*v3io.Container, error) {
	// create context
	if workers == 0 {
		workers = 8
	}

	context, err := v3io.NewContext(logger, addr, workers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create client")
	}

	// create session
	session, err := context.NewSessionFromConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create session")
	}

	// create the container
	container, err := session.NewContainer(cont)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create container")
	}

	return container, nil
}

// AsInt64Array convert v3io blob to Int array
func AsInt64Array(val []byte) []uint64 {
	var array []uint64
	bytes := val
	for i := 16; i+8 <= len(bytes); i += 8 {
		val := binary.LittleEndian.Uint64(bytes[i : i+8])
		array = append(array, val)
	}
	return array
}

// DeleteTable deletes a table
func DeleteTable(logger logger.Logger, container *v3io.Container, path, filter string, workers int) error {

	input := v3io.GetItemsInput{Path: path, AttributeNames: []string{"__name"}, Filter: filter}
	iter, err := NewAsyncItemsCursor(container, &input, workers, []string{}, logger, 0)
	//iter, err := container.Sync.GetItemsCursor(&input)
	if err != nil {
		return err
	}

	responseChan := make(chan *v3io.Response, 1000)
	commChan := make(chan int, 2)
	doneChan := respWaitLoop(logger, commChan, responseChan, 10*time.Second)
	reqMap := map[uint64]bool{}

	i := 0
	for iter.Next() {
		name := iter.GetField("__name").(string)
		req, err := container.DeleteObject(&v3io.DeleteObjectInput{
			Path: path + "/" + url.QueryEscape(name)}, nil, responseChan)
		if err != nil {
			commChan <- i
			return errors.Wrap(err, "failed to delete object "+name)
		}
		reqMap[req.ID] = true
		i++
	}

	commChan <- i
	if iter.Err() != nil {
		return errors.Wrap(iter.Err(), "failed to delete object ")
	}

	<-doneChan

	err = container.Sync.DeleteObject(&v3io.DeleteObjectInput{Path: path})
	if err != nil {
		if !utils.IsNotExistsError(err) {
			return errors.Wrapf(err, "Failed to delete table object '%s'.", path)
		}
	}

	return nil
}

func respWaitLoop(logger logger.Logger, comm chan int, responseChan chan *v3io.Response, timeout time.Duration) chan bool {
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
					logger.ErrorWith("failed Delete response", "error", resp.Error)
					// TODO: signal done and return?
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
					logger.ErrorWith("Resp loop timed out!", "requests", requests, "response", responses)
					done <- true
					return
				}
				active = false
			}
		}
	}()

	return done
}

func ProcessPaths(session *frames.Session, path string, addSlash bool) (string, string, error) {

	container := session.Container

	if container == "" {
		sp := strings.SplitN(path, "/", 2)
		if len(sp) < 2 {
			return "", "", errors.New("Please specify a data container name via the container parameters or the path prefix e.g. bigdata/mytable")
		}
		container = sp[0]
		path = sp[1]
	}

	if container == v3ioHomeVar {
		container = v3ioUsersContainer
		path = session.User + "/" + path
	}

	if strings.HasPrefix(path, v3ioHomeVar+"/") {
		container = v3ioUsersContainer
		path = session.User + "/" + path[len(v3ioHomeVar)+1:]
	}

	if addSlash && !strings.HasSuffix(path, "/") {
		path += "/"
	}

	return container, path, nil
}
