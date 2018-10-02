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
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"strings"
)

type RootCommandeer struct {
	adapter     *tsdb.V3ioAdapter
	logger      logger.Logger
	v3iocfg     *config.V3ioConfig
	cmd         *cobra.Command
	v3ioPath    string
	dbPath      string
	cfgFilePath string
	logLevel    string
	container   string
	username    string
	password    string
	Reporter    *performance.MetricReporter
}

func NewRootCommandeer() *RootCommandeer {
	commandeer := &RootCommandeer{}

	cmd := &cobra.Command{
		Use:          "tsdbctl [command]",
		Short:        "V3IO TSDB command-line interface",
		SilenceUsage: true,
	}

	defaultV3ioServer := os.Getenv("V3IO_SERVICE_URL")

	cmd.PersistentFlags().StringVarP(&commandeer.logLevel, "log-level", "v", "", "Logging level")
	cmd.PersistentFlags().Lookup("log-level").NoOptDefVal = "info"
	cmd.PersistentFlags().StringVarP(&commandeer.dbPath, "table-path", "t", "", "sub path for the TSDB, inside the container")
	cmd.PersistentFlags().StringVarP(&commandeer.v3ioPath, "server", "s", defaultV3ioServer, "V3IO Service URL - ip:port")
	cmd.PersistentFlags().StringVarP(&commandeer.cfgFilePath, "config", "g", "", "path to yaml config file")
	cmd.PersistentFlags().StringVarP(&commandeer.container, "container", "c", "", "container to use")
	cmd.PersistentFlags().StringVarP(&commandeer.username, "username", "u", "", "user name")
	cmd.PersistentFlags().StringVarP(&commandeer.password, "password", "p", "", "password")

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
	cfg, err := config.GetOrLoadFromFile(rc.cfgFilePath)
	if err != nil {
		// if we couldn't load the file and its not the default
		if rc.cfgFilePath == "" {
			return errors.Wrap(err, "Failed to load configuration")
		} else {
			return errors.Wrap(err, fmt.Sprintf("Failed to load config from '%s'", rc.cfgFilePath))
		}
	}
	return rc.populateConfig(cfg)
}

func (rc *RootCommandeer) populateConfig(cfg *config.V3ioConfig) error {
	// Initialize performance monitoring
	// TODO: support custom report writers (file, syslog, prometheus, etc.)
	rc.Reporter = performance.ReporterInstanceFromConfig(cfg)

	if rc.username != "" {
		cfg.Username = rc.username
	}

	if rc.password != "" {
		cfg.Password = rc.password
	}

	if rc.v3ioPath != "" {
		// read username and password
		if i := strings.LastIndex(rc.v3ioPath, "@"); i > 0 {
			usernameAndPassword := rc.v3ioPath[0:i]
			rc.v3ioPath = rc.v3ioPath[i+1:]
			if userpass := strings.Split(usernameAndPassword, ":"); len(userpass) > 1 {
				fmt.Printf("Debug: up0=%s up1=%s u=%s p=%s\n", userpass[0], userpass[1], rc.username, rc.password)
				if userpass[0] != "" && rc.username != "" {
					return fmt.Errorf("username should only be defined once")
				} else {
					cfg.Username = userpass[0]
				}

				if userpass[1] != "" && rc.password != "" {
					return fmt.Errorf("password should only be defined once")
				} else {
					cfg.Password = userpass[1]
				}
			} else {
				if usernameAndPassword != "" && rc.username != "" {
					return fmt.Errorf("username should only be defined once")
				} else {
					cfg.Username = usernameAndPassword
				}
			}
		}

		slash := strings.LastIndex(rc.v3ioPath, "/")
		if slash == -1 || len(rc.v3ioPath) <= slash+1 {
			if rc.container != "" {
				cfg.Container = rc.container
			} else if cfg.Container == "" {
				return fmt.Errorf("missing container name in V3IO URL")
			}
			cfg.WebApiEndpoint = rc.v3ioPath
		} else {
			cfg.WebApiEndpoint = rc.v3ioPath[0:slash]
			cfg.Container = rc.v3ioPath[slash+1:]
		}
	}
	if rc.container != "" {
		cfg.Container = rc.container
	}
	if rc.dbPath != "" {
		cfg.TablePath = rc.dbPath
	}
	if cfg.WebApiEndpoint == "" || cfg.Container == "" || cfg.TablePath == "" {
		return fmt.Errorf("user must provide V3IO URL, container name, and table path via the config file or flags")
	}
	if rc.logLevel != "" {
		cfg.LogLevel = rc.logLevel
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
