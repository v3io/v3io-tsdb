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

package tsdbctl

import (
	"os"

	"fmt"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"strings"
)

const defaultMinimumSampleSize, defaultMaximumSampleSize = 2, 8     // bytes
const defaultMaximumPartitionSize = 1700000                         // 1.7MB
const defaultMinimumChunkSize, defaultMaximumChunkSize = 200, 62000 // bytes

type RootCommandeer struct {
	adapter     *tsdb.V3ioAdapter
	logger      logger.Logger
	v3iocfg     *config.V3ioConfig
	cmd         *cobra.Command
	v3ioPath    string
	dbPath      string
	cfgFilePath string
	verbose     string
}

func NewRootCommandeer() *RootCommandeer {
	commandeer := &RootCommandeer{}

	cmd := &cobra.Command{
		Use:          "tsdbctl [command]",
		Short:        "V3IO TSDB command-line interface",
		SilenceUsage: true,
	}

	defaultV3ioServer := os.Getenv("V3IO_SERVICE_URL")

	cmd.PersistentFlags().StringVarP(&commandeer.verbose, "verbose", "v", "", "Verbose output")
	cmd.PersistentFlags().Lookup("verbose").NoOptDefVal = "debug"
	cmd.PersistentFlags().StringVarP(&commandeer.dbPath, "dbpath", "p", "", "sub path for the TSDB, inside the container")
	cmd.PersistentFlags().StringVarP(&commandeer.v3ioPath, "server", "s", defaultV3ioServer, "V3IO Service URL - username:password@ip:port/container")
	cmd.PersistentFlags().StringVarP(&commandeer.cfgFilePath, "config", "c", "", "path to yaml config file")

	// add children
	cmd.AddCommand(
		newAddCommandeer(commandeer).cmd,
		newQueryCommandeer(commandeer).cmd,
		newTimeCommandeer(commandeer).cmd,
		newCreateCommandeer(commandeer).cmd,
		newInfoCommandeer(commandeer).cmd,
		newDeleteCommandeer(commandeer).cmd,
		newCheckCommandeer(commandeer).cmd,
	)

	commandeer.cmd = cmd

	return commandeer
}

// Execute uses os.Args to execute the command
func (rc *RootCommandeer) Execute() error {
	return rc.cmd.Execute()
}

// GetCmd returns the underlying cobra command
func (rc *RootCommandeer) GetCmd() *cobra.Command {
	return rc.cmd
}

// CreateMarkdown generates MD files in the target path
func (rc *RootCommandeer) CreateMarkdown(path string) error {
	return doc.GenMarkdownTree(rc.cmd, path)
}

func (rc *RootCommandeer) initialize() error {
	cfg, err := config.LoadConfig(rc.cfgFilePath)
	if err != nil {
		// if we couldn't load the file and its not the default
		if rc.cfgFilePath != "" {
			return errors.Wrap(err, "Failed to load config from file "+rc.cfgFilePath)
		}
		cfg = &config.V3ioConfig{} // initialize struct, will try and set it from individual flags
		config.InitDefaults(cfg)
	}
	return rc.populateConfig(cfg)
}

func (rc *RootCommandeer) populateConfig(cfg *config.V3ioConfig) error {
	if rc.v3ioPath != "" {
		// read username and password
		if i := strings.LastIndex(rc.v3ioPath, "@"); i > 0 {
			cfg.Username = rc.v3ioPath[0:i]
			rc.v3ioPath = rc.v3ioPath[i+1:]
			if userpass := strings.Split(cfg.Username, ":"); len(userpass) > 1 {
				cfg.Username = userpass[0]
				cfg.Password = userpass[1]
			}
		}

		slash := strings.LastIndex(rc.v3ioPath, "/")
		if slash == -1 || len(rc.v3ioPath) <= slash+1 {
			return fmt.Errorf("missing container name in V3IO URL")
		}
		cfg.V3ioUrl = rc.v3ioPath[0:slash]
		cfg.Container = rc.v3ioPath[slash+1:]
	}
	if rc.dbPath != "" {
		cfg.Path = rc.dbPath
	}
	if cfg.V3ioUrl == "" || cfg.Container == "" || cfg.Path == "" {
		return fmt.Errorf("user must provide V3IO URL, container name, and table path via the config file or flags")
	}
	if rc.verbose != "" {
		cfg.Verbose = rc.verbose
	}
	if cfg.MaximumChunkSize == 0 {
		cfg.MaximumChunkSize = defaultMaximumChunkSize
	}
	if cfg.MinimumChunkSize == 0 {
		cfg.MinimumChunkSize = defaultMinimumChunkSize
	}
	if cfg.MaximumSampleSize == 0 {
		cfg.MaximumSampleSize = defaultMaximumSampleSize
	}
	if cfg.MinimumSampleSize == 0 {
		cfg.MinimumSampleSize = defaultMinimumSampleSize
	}
	if cfg.MaximumPartitionSize == 0 {
		cfg.MaximumPartitionSize = defaultMaximumPartitionSize
	}
	rc.v3iocfg = cfg
	return nil
}

func (rc *RootCommandeer) startAdapter() error {
	var err error
	rc.adapter, err = tsdb.NewV3ioAdapter(rc.v3iocfg, nil, nil)
	if err != nil {
		return errors.Wrap(err, "Failed to start TSDB Adapter")
	}

	rc.logger = rc.adapter.GetLogger("cli")

	return nil

}
