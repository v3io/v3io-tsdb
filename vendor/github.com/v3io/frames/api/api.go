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

package api

// API Layer

import (
	"fmt"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"

	// Load backends (make sure they register)
	_ "github.com/v3io/frames/backends/csv"
	_ "github.com/v3io/frames/backends/kv"
	_ "github.com/v3io/frames/backends/stream"
	_ "github.com/v3io/frames/backends/tsdb"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"strings"
)

const (
	missingMsg = "missing parameters"
)

// API layer, implements common CRUD operations
// TODO: Call it DAL? (data access layer)
type API struct {
	logger   logger.Logger
	backends map[string]frames.DataBackend
	config   *frames.Config
}

// New returns a new API layer struct
func New(logger logger.Logger, config *frames.Config) (*API, error) {
	if logger == nil {
		var err error
		logger, err = frames.NewLogger(config.Log.Level)
		if err != nil {
			return nil, errors.Wrap(err, "can't create logger")
		}
	}

	api := &API{
		logger: logger,
		config: config,
	}

	if err := api.createBackends(config); err != nil {
		msg := "can't create backends"
		api.logger.ErrorWith(msg, "error", err, "config", config)
		return nil, errors.Wrap(err, "can't create backends")
	}

	return api, nil
}

// Read reads from database, emitting results to wf
func (api *API) Read(request *frames.ReadRequest, out chan frames.Frame) error {
	api.logger.InfoWith("read request", "request", request)

	backend, ok := api.backends[request.Backend]

	if !ok {
		api.logger.ErrorWith("unknown backend", "name", request.Backend)
		return fmt.Errorf("unknown backend - %q", request.Backend)
	}

	iter, err := backend.Read(request)
	if err != nil {
		api.logger.ErrorWith("can't query", "error", err)
		return errors.Wrap(err, "can't query")
	}

	for iter.Next() {
		out <- iter.At()
	}

	if err := iter.Err(); err != nil {
		msg := "error during iteration"
		api.logger.ErrorWith(msg, "error", err)
		return errors.Wrap(err, msg)
	}

	return nil
}

// Write write data to backend, returns num_frames, num_rows, error
func (api *API) Write(request *frames.WriteRequest, in chan frames.Frame) (int, int, error) {
	if request.Backend == "" || request.Table == "" {
		api.logger.ErrorWith(missingMsg, "request", request)
		return -1, -1, fmt.Errorf(missingMsg)
	}

	api.logger.InfoWith("write request", "request", request)
	backend, ok := api.backends[request.Backend]
	if !ok {
		api.logger.ErrorWith("unkown backend", "name", request.Backend)
		return -1, -1, fmt.Errorf("unknown backend - %s", request.Backend)
	}

	appender, err := backend.Write(request)
	if err != nil {
		msg := "backend Write failed"
		api.logger.ErrorWith(msg, "error", err)
		return -1, -1, errors.Wrap(err, msg)
	}

	nFrames, nRows := 0, 0
	if request.ImmidiateData != nil {
		nFrames, nRows = 1, request.ImmidiateData.Len()
	}

	for frame := range in {
		api.logger.DebugWith("frame to write", "size", frame.Len())
		if err := appender.Add(frame); err != nil {
			msg := "can't add frame"
			api.logger.ErrorWith(msg, "error", err)
			if strings.Contains(err.Error(), "Failed POST with status 401") {
				err = errors.New("unauthorized update (401), may be caused by wrong password or credentials")
			}
			return nFrames, nRows, errors.Wrap(err, msg)
		}

		nFrames++
		nRows += frame.Len()
		api.logger.DebugWith("write", "numFrames", nFrames, "numRows", nRows)
	}

	api.logger.Debug("write done")

	// TODO: Specify timeout in request?
	if nRows > 0 {
		if err := appender.WaitForComplete(time.Duration(api.config.DefaultTimeout) * time.Second); err != nil {
			msg := "can't wait for completion"
			api.logger.ErrorWith(msg, "error", err)
			return nFrames, nRows, errors.Wrap(err, msg)
		}
	} else {
		api.logger.DebugWith("write request with zero rows", "frames", nFrames, "requst", request)
	}

	return nFrames, nRows, nil
}

// Create will create a new table
func (api *API) Create(request *frames.CreateRequest) error {
	if request.Backend == "" || request.Table == "" {
		api.logger.ErrorWith(missingMsg, "request", request)
		return fmt.Errorf(missingMsg)
	}

	api.logger.DebugWith("create", "request", request)
	backend, ok := api.backends[request.Backend]
	if !ok {
		api.logger.ErrorWith("unkown backend", "name", request.Backend)
		return fmt.Errorf("unknown backend - %s", request.Backend)
	}

	if err := backend.Create(request); err != nil {
		api.logger.ErrorWith("error creating table", "error", err, "request", request)
		return errors.Wrap(err, "error creating table")
	}

	return nil
}

// Delete deletes a table or part of it
func (api *API) Delete(request *frames.DeleteRequest) error {
	if request.Backend == "" || request.Table == "" {
		api.logger.ErrorWith(missingMsg, "request", request)
		return fmt.Errorf(missingMsg)
	}

	api.logger.DebugWith("delete", "request", request)
	backend, ok := api.backends[request.Backend]
	if !ok {
		api.logger.ErrorWith("unkown backend", "name", request.Backend)
		return fmt.Errorf("unknown backend - %s", request.Backend)
	}

	if err := backend.Delete(request); err != nil {
		api.logger.ErrorWith("error deleting table", "error", err, "request", request)
		return errors.Wrap(err, "can't delete")
	}

	return nil
}

// Exec executes a command on the backend
func (api *API) Exec(request *frames.ExecRequest) (frames.Frame, error) {
	if request.Backend == "" || request.Table == "" {
		api.logger.ErrorWith(missingMsg, "request", request)
		return nil, fmt.Errorf(missingMsg)
	}

	// TODO: This print session in clear text
	//	api.logger.DebugWith("exec", "request", request)
	backend, ok := api.backends[request.Backend]
	if !ok {
		api.logger.ErrorWith("unkown backend", "name", request.Backend)
		return nil, fmt.Errorf("unknown backend - %s", request.Backend)
	}

	frame, err := backend.Exec(request)
	if err != nil {
		api.logger.ErrorWith("error in exec", "error", err, "request", request)
		return nil, errors.Wrap(err, "can't exec")
	}

	return frame, nil
}

func (api *API) populateQuery(request *frames.ReadRequest) error {
	sqlQuery, err := frames.ParseSQL(request.Query)
	if err != nil {
		return errors.Wrap(err, "bad SQL query")
	}

	if request.Table != "" {
		return fmt.Errorf("both query AND table provided")
	}
	request.Table = sqlQuery.Table

	if request.Columns != nil {
		return fmt.Errorf("both query AND columns provided")
	}
	request.Columns = sqlQuery.Columns

	if request.Filter != "" {
		return fmt.Errorf("both query AND filter provided")
	}
	request.Filter = sqlQuery.Filter

	if request.GroupBy != "" {
		return fmt.Errorf("both query AND group_by provided")
	}
	request.GroupBy = sqlQuery.GroupBy

	return nil
}

func (api *API) createBackends(config *frames.Config) error {
	api.backends = make(map[string]frames.DataBackend)

	for _, cfg := range config.Backends {
		factory := backends.GetFactory(cfg.Type)
		if factory == nil {
			return fmt.Errorf("unknown backend - %q", cfg.Type)
		}

		backend, err := factory(api.logger, cfg, config)
		if err != nil {
			return errors.Wrapf(err, "%s:%s - can't create backend", cfg.Name, cfg.Type)
		}

		api.backends[cfg.Name] = backend
	}

	return nil
}
