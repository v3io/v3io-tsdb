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

package stream

import (
	"encoding/json"
	"time"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"

	"github.com/v3io/frames"
)

func (b *Backend) Write(request *frames.WriteRequest) (frames.FrameAppender, error) {

	container, tablePath, err := b.newConnection(request.Session, request.Table, true)
	if err != nil {
		return nil, err
	}

	appender := streamAppender{
		request:      request,
		container:    container,
		tablePath:    tablePath,
		responseChan: make(chan *v3io.Response, 1000),
		commChan:     make(chan int, 2),
		logger:       b.logger,
	}

	if request.ImmidiateData != nil {
		err := appender.Add(request.ImmidiateData)
		if err != nil {
			return &appender, err
		}
	}

	return &appender, nil
}

type streamAppender struct {
	request      *frames.WriteRequest
	container    *v3io.Container
	tablePath    string
	responseChan chan *v3io.Response
	commChan     chan int
	logger       logger.Logger
}

// TODO: make it async
func (a *streamAppender) Add(frame frames.Frame) error {
	records := make([]*v3io.StreamRecord, 0, frame.Len())
	iter := frame.IterRows(true)
	for iter.Next() {

		body, err := json.Marshal(iter.Row())
		if err != nil {
			return err
		}
		records = append(records, &v3io.StreamRecord{Data: body})
	}

	if err := iter.Err(); err != nil {
		return errors.Wrap(err, "row iteration error")
	}

	_, err := a.container.Sync.PutRecords(&v3io.PutRecordsInput{
		Path: a.tablePath, Records: records})

	return err
}

func (a *streamAppender) WaitForComplete(timeout time.Duration) error {
	return nil
}
