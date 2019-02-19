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
	"fmt"
	"reflect"
	"strings"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"
	"github.com/v3io/frames/v3ioutils"
	v3io "github.com/v3io/v3io-go-http"
)

// Backend is a tsdb backend
type Backend struct {
	backendConfig *frames.BackendConfig
	framesConfig  *frames.Config
	logger        logger.Logger
}

// NewBackend return a new v3io stream backend
func NewBackend(logger logger.Logger, cfg *frames.BackendConfig, framesConfig *frames.Config) (frames.DataBackend, error) {

	frames.InitBackendDefaults(cfg, framesConfig)
	newBackend := Backend{
		logger:        logger.GetChild("stream"),
		backendConfig: cfg,
		framesConfig:  framesConfig,
	}

	return &newBackend, nil
}

// Create creates a table
func (b *Backend) Create(request *frames.CreateRequest) error {

	// TODO: check if Stream exist, if it already has the desired params can silently ignore, may need a -silent flag

	var isInt bool
	attrs := request.Attributes()
	shards := int64(1)

	shardsVar, ok := attrs["shards"]
	if ok {
		shards, isInt = shardsVar.(int64)
		fmt.Println(reflect.TypeOf(shardsVar))
		if !isInt || shards < 1 {
			return errors.Errorf("Shards attribute must be a positive integer (got %v)", shardsVar)
		}
	}

	retention := int64(24)
	retentionVar, ok := attrs["retention_hours"]
	if ok {
		retention, isInt = retentionVar.(int64)
		if !isInt || retention < 1 {
			return errors.Errorf("retention_hours attribute must be a positive integer (got %v)", retentionVar)
		}
	}

	container, path, err := b.newConnection(request.Session, request.Table, true)
	if err != nil {
		return err
	}

	err = container.Sync.CreateStream(&v3io.CreateStreamInput{
		Path: path, ShardCount: int(shards), RetentionPeriodHours: int(retention)})
	if err != nil {
		b.logger.ErrorWith("CreateStream failed", "path", path, "err", err)
	}

	return nil
}

// Delete deletes a table or part of it
func (b *Backend) Delete(request *frames.DeleteRequest) error {

	container, path, err := b.newConnection(request.Session, request.Table, true)
	if err != nil {
		return err
	}

	err = container.Sync.DeleteStream(&v3io.DeleteStreamInput{Path: path})
	if err != nil {
		b.logger.ErrorWith("DeleteStream failed", "path", path, "err", err)
	}

	return nil
}

// Exec executes a command
func (b *Backend) Exec(request *frames.ExecRequest) (frames.Frame, error) {
	cmd := strings.TrimSpace(strings.ToLower(request.Command))
	switch cmd {
	case "put":
		return nil, b.put(request)
	}
	return nil, fmt.Errorf("Stream backend does not support commend - %s", cmd)
}

func (b *Backend) put(request *frames.ExecRequest) error {

	varData, hasData := request.Args["data"]
	if !hasData || request.Table == "" {
		return fmt.Errorf("table name and data parameter must be specified")
	}
	data := varData.GetSval()

	clientInfo := ""
	if val, ok := request.Args["clientinfo"]; ok {
		clientInfo = val.GetSval()
	}

	partitionKey := ""
	if val, ok := request.Args["partition"]; ok {
		partitionKey = val.GetSval()
	}

	container, path, err := b.newConnection(request.Session, request.Table, true)
	if err != nil {
		return err
	}

	b.logger.DebugWith("put record", "path", path, "len", len(data), "client", clientInfo, "partition", partitionKey)
	records := []*v3io.StreamRecord{{
		Data: []byte(data), ClientInfo: []byte(clientInfo), PartitionKey: partitionKey,
	}}
	_, err = container.Sync.PutRecords(&v3io.PutRecordsInput{
		Path:    path,
		Records: records,
	})

	return err
}

func (b *Backend) newConnection(session *frames.Session, path string, addSlash bool) (*v3io.Container, string, error) {

	session = frames.InitSessionDefaults(session, b.framesConfig)
	containerName, newPath, err := v3ioutils.ProcessPaths(session, path, addSlash)
	if err != nil {
		return nil, "", err
	}

	session.Container = containerName
	container, err := v3ioutils.NewContainer(
		session,
		b.logger,
		b.backendConfig.Workers,
	)

	return container, newPath, err
}

func init() {
	if err := backends.Register("stream", NewBackend); err != nil {
		panic(err)
	}
}
