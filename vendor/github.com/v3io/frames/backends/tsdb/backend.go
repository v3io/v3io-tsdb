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

package tsdb

import (
	"fmt"
	"strings"

	"github.com/nuclio/logger"

	"github.com/v3io/frames"
	"github.com/v3io/frames/backends"

	"github.com/pkg/errors"
	"github.com/v3io/frames/v3ioutils"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	tsdbutils "github.com/v3io/v3io-tsdb/pkg/utils"
)

// Backend is a tsdb backend
type Backend struct {
	adapters      map[string]*tsdb.V3ioAdapter
	backendConfig *frames.BackendConfig
	framesConfig  *frames.Config
	logger        logger.Logger
}

// NewBackend return a new tsdb backend
func NewBackend(logger logger.Logger, cfg *frames.BackendConfig, framesConfig *frames.Config) (frames.DataBackend, error) {

	frames.InitBackendDefaults(cfg, framesConfig)
	newBackend := Backend{
		adapters:      map[string]*tsdb.V3ioAdapter{},
		logger:        logger.GetChild("tsdb"),
		backendConfig: cfg,
		framesConfig:  framesConfig,
	}

	return &newBackend, nil
}

func (b *Backend) newConfig(session *frames.Session) *config.V3ioConfig {

	cfg := &config.V3ioConfig{
		WebApiEndpoint: session.Url,
		Container:      session.Container,
		Username:       session.User,
		Password:       session.Password,
		Workers:        b.backendConfig.Workers,
		LogLevel:       b.framesConfig.Log.Level,
	}
	return config.WithDefaults(cfg)
}

func (b *Backend) newAdapter(session *frames.Session, path string) (*tsdb.V3ioAdapter, error) {

	session = frames.InitSessionDefaults(session, b.framesConfig)
	containerName, newPath, err := v3ioutils.ProcessPaths(session, path, false)
	if err != nil {
		return nil, err
	}

	session.Container = containerName
	cfg := b.newConfig(session)

	container, err := v3ioutils.NewContainer(
		session,
		b.logger,
		cfg.Workers,
	)

	if err != nil {
		return nil, err
	}

	cfg.TablePath = newPath
	b.logger.DebugWith("tsdb config", "config", cfg)
	adapter, err := tsdb.NewV3ioAdapter(cfg, container, b.logger)
	if err != nil {
		return nil, err
	}

	return adapter, nil
}

// GetAdapter returns an adapter
func (b *Backend) GetAdapter(session *frames.Session, path string) (*tsdb.V3ioAdapter, error) {
	// TODO: maintain adapter cache, for now create new per read/write request

	adapter, err := b.newAdapter(session, path)
	if err != nil {
		return nil, err
	}
	return adapter, nil
}

// Create creates a table
func (b *Backend) Create(request *frames.CreateRequest) error {

	attrs := request.Attributes()

	attr, ok := attrs["rate"]
	if !ok {
		return errors.New("Must specify 'rate' attribute to specify maximum sample rate, e.g. '1/m'")
	}
	rate, isStr := attr.(string)
	if !isStr {
		return errors.New("'rate' attribute must be a string, e.g. '1/m'")
	}

	aggregationGranularity := config.DefaultAggregationGranularity
	attr, ok = attrs["aggregation-granularity"]
	if ok {
		val, isStr := attr.(string)
		if !isStr {
			return errors.New("'aggregation-granularity' attribute must be a string")
		}
		aggregationGranularity = val
	}

	defaultRollups := ""
	attr, ok = attrs["aggregates"]
	if ok {
		val, isStr := attr.(string)
		if !isStr {
			return errors.New("'aggregates' attribute must be a string")
		}
		defaultRollups = val
	}

	session := frames.InitSessionDefaults(request.Session, b.framesConfig)
	containerName, path, err := v3ioutils.ProcessPaths(session, request.Table, false)
	if err != nil {
		return err
	}

	session.Container = containerName
	cfg := b.newConfig(session)

	cfg.TablePath = path
	dbSchema, err := schema.NewSchema(cfg, rate, aggregationGranularity, defaultRollups)

	if err != nil {
		return errors.Wrap(err, "Failed to create a TSDB schema.")
	}

	err = tsdb.CreateTSDB(cfg, dbSchema)
	if b.ignoreCreateExists(request, err) {
		return nil
	}
	return err
}

// Delete deletes a table or part of it
func (b *Backend) Delete(request *frames.DeleteRequest) error {

	start, err := tsdbutils.Str2duration(request.Start)
	if err != nil {
		return err
	}

	end, err := tsdbutils.Str2duration(request.End)
	if err != nil {
		return err
	}

	delAll := request.Start == "" && request.End == ""

	adapter, err := b.GetAdapter(request.Session, request.Table)
	if err != nil {
		return err
	}

	err = adapter.DeleteDB(delAll, false, start, end)
	if err == nil {
		return err
	}

	if tsdbutils.IsNotExistsError(err) && request.IfMissing == frames.IgnoreError {
		return nil
	}
	return err

}

// Exec executes a command
func (b *Backend) Exec(request *frames.ExecRequest) (frames.Frame, error) {
	return nil, fmt.Errorf("TSDB backend does not support Exec")
}

func (b *Backend) ignoreCreateExists(request *frames.CreateRequest, err error) bool {
	if request.IfExists != frames.IgnoreError {
		return false
	}

	// TODO: Ask tsdb to return specific error value, this is brittle
	return strings.Contains(err.Error(), "A TSDB table already exists")
}

func init() {
	if err := backends.Register("tsdb", NewBackend); err != nil {
		panic(err)
	}
}
