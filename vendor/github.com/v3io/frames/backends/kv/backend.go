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

package kv

import (
	"fmt"
	"strings"

	"github.com/nuclio/logger"
	"github.com/v3io/v3io-go-http"

	"github.com/v3io/frames"
	"github.com/v3io/frames/v3ioutils"
)

// Backend is key/value backend
type Backend struct {
	logger       logger.Logger
	numWorkers   int
	framesConfig *frames.Config
}

// NewBackend return a new key/value backend
func NewBackend(logger logger.Logger, config *frames.BackendConfig, framesConfig *frames.Config) (frames.DataBackend, error) {

	frames.InitBackendDefaults(config, framesConfig)
	newBackend := Backend{
		logger:       logger.GetChild("kv"),
		numWorkers:   config.Workers,
		framesConfig: framesConfig,
	}

	return &newBackend, nil
}

// Create creates a table
func (b *Backend) Create(request *frames.CreateRequest) error {
	return fmt.Errorf("not requiered, table is created on first write")
}

// Delete deletes a table (or part of it)
func (b *Backend) Delete(request *frames.DeleteRequest) error {

	container, path, err := b.newConnection(request.Session, request.Table, true)
	if err != nil {
		return err
	}

	return v3ioutils.DeleteTable(b.logger, container, path, request.Filter, b.numWorkers)
	// TODO: delete the table directory entry if filter == ""
}

// Exec executes a command
func (b *Backend) Exec(request *frames.ExecRequest) (frames.Frame, error) {
	cmd := strings.TrimSpace(strings.ToLower(request.Command))
	switch cmd {
	case "infer", "inferschema":
		return nil, b.inferSchema(request)
	case "update":
		return nil, b.updateItem(request)
	}
	return nil, fmt.Errorf("KV backend does not support commend - %s", cmd)
}

func (b *Backend) updateItem(request *frames.ExecRequest) error {
	varKey, hasKey := request.Args["key"]
	varExpr, hasExpr := request.Args["expression"]
	if !hasExpr || !hasKey || request.Table == "" {
		return fmt.Errorf("table, key and expression parameters must be specified")
	}

	key := varKey.GetSval()
	expr := varExpr.GetSval()

	condition := ""
	if val, ok := request.Args["condition"]; ok {
		condition = val.GetSval()
	}

	container, path, err := b.newConnection(request.Session, request.Table, true)
	if err != nil {
		return err
	}

	b.logger.DebugWith("update item", "path", path, "key", key, "expr", expr, "condition", condition)
	return container.Sync.UpdateItem(&v3io.UpdateItemInput{
		Path: path + key, Expression: &expr, Condition: condition})
}

/*func (b *Backend) newContainer(session *frames.Session) (*v3io.Container, error) {

	container, err := v3ioutils.NewContainer(
		session,
		b.framesConfig,
		b.logger,
		b.numWorkers,
	)

	return container, err
}

*/

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
		b.numWorkers,
	)

	return container, newPath, err
}
