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
		Use:          "tsdbctl [command] [arguments] [flags]",
		Short:        "V3IO TSDB command-line interface (CLI)",
		SilenceUsage: true,
	}

	defaultV3ioServer := os.Getenv("V3IO_SERVICE_URL")

	cmd.PersistentFlags().StringVarP(&commandeer.logLevel, "log-level", "v", "",
		"Verbose output. Add \"=<level>\" to set the log level -\ndebug | info | warn | error. For example: -v=warn.\n(default - \""+config.DefaultVerboseLevel+"\" when using the flag; \""+config.DefaultLogLevel+"\" otherwise)")
	cmd.PersistentFlags().Lookup("log-level").NoOptDefVal = config.DefaultVerboseLevel
	cmd.PersistentFlags().StringVarP(&commandeer.dbPath, "table-path", "t", "",
		"[Required] Path to the TSDB table within the configured\ndata container. Examples: \"mytsdb\"; \"/my_tsdbs/tsdbd1\".")
	// We don't enforce this flag (cmd.MarkFlagRequired("table-path")),
	// although it's documented as Required, because this flag isn't required
	// for the hidden `time` command + during internal tests we might want to
	// configure the table path in a configuration file.
	cmd.PersistentFlags().StringVarP(&commandeer.v3ioPath, "server", "s", defaultV3ioServer,
		"Web-gateway (web-APIs) service endpoint of an instance of\nthe Iguazio Continuous Data Platform, of the format\n\"<IP address>:<port number=8081>\". Examples: \"localhost:8081\"\n(when running on the target platform); \"192.168.1.100:8081\".")
	cmd.PersistentFlags().StringVarP(&commandeer.cfgFilePath, "config", "g", "",
		"Path to a YAML TSDB configuration file. When this flag isn't\nset, the CLI checks for a "+config.DefaultConfigurationFileName+" configuration\nfile in the current directory. CLI flags override file\nconfigurations. Example: \"~/cfg/my_v3io_tsdb_cfg.yaml\".")
	cmd.PersistentFlags().StringVarP(&commandeer.container, "container", "c", "",
		"The name of an Iguazio Continuous Data Platform data container\nin which to create the TSDB table. Example: \"bigdata\".")
	cmd.PersistentFlags().StringVarP(&commandeer.username, "username", "u", "",
		"Username of an Iguazio Continuous Data Platform user.")
	cmd.PersistentFlags().StringVarP(&commandeer.password, "password", "p", "",
		"Password of the configured user (see -u|--username).")

	// Add children
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

// Execute the command using os.Args
func (rc *RootCommandeer) Execute() error {
	return rc.cmd.Execute()
}

// Return the underlying Cobra command
func (rc *RootCommandeer) GetCmd() *cobra.Command {
	return rc.cmd
}

// Generate Markdown files in the target path
func (rc *RootCommandeer) CreateMarkdown(path string) error {
	return doc.GenMarkdownTree(rc.cmd, path)
}

func (rc *RootCommandeer) initialize() error {
	cfg, err := config.GetOrLoadFromFile(rc.cfgFilePath)
	if err != nil {
		// Display an error if we fail to load a configuration file
		if rc.cfgFilePath == "" {
			return errors.Wrap(err, "Failed to load the TSDB configuration.")
		} else {
			return errors.Wrap(err, fmt.Sprintf("Failed to load the TSDB configuration from '%s'.", rc.cfgFilePath))
		}
	}
	return rc.populateConfig(cfg)
}

func (rc *RootCommandeer) populateConfig(cfg *config.V3ioConfig) error {
	// Initialize performance monitoring
	// TODO: support custom report writers (file, syslog, Prometheus, etc.)
	rc.Reporter = performance.ReporterInstanceFromConfig(cfg)

	if rc.username != "" {
		cfg.Username = rc.username
	}

	if rc.password != "" {
		cfg.Password = rc.password
	}

	if rc.v3ioPath != "" {
		// Check for username and password in the web-gateway service endpoint
		// (supported for backwards compatibility)
		if i := strings.LastIndex(rc.v3ioPath, "@"); i > 0 {
			usernameAndPassword := rc.v3ioPath[0:i]
			rc.v3ioPath = rc.v3ioPath[i+1:]
			if userpass := strings.Split(usernameAndPassword, ":"); len(userpass) > 1 {
				fmt.Printf("Debug: up0=%s up1=%s u=%s p=%s\n", userpass[0], userpass[1], rc.username, rc.password)
				if userpass[0] != "" && rc.username != "" {
					return fmt.Errorf("Username should only be defined once.")
				} else {
					cfg.Username = userpass[0]
				}

				if userpass[1] != "" && rc.password != "" {
					return fmt.Errorf("Password should only be defined once.")
				} else {
					cfg.Password = userpass[1]
				}
			} else {
				if usernameAndPassword != "" && rc.username != "" {
					return fmt.Errorf("Username should only be defined once.")
				} else {
					cfg.Username = usernameAndPassword
				}
			}
		}

		// Check for a container name in the in the web-gateway service endpoint
		// (supported for backwards compatibility)
		slash := strings.LastIndex(rc.v3ioPath, "/")
		if slash == -1 || len(rc.v3ioPath) <= slash+1 {
			if rc.container != "" {
				cfg.Container = rc.container
			} else if cfg.Container == "" {
				return fmt.Errorf("Missing the name of the TSDB's parent data container.")
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
		return fmt.Errorf("Not all required configuration information was provided. The endpoint of the web-gateway service, related username and password authentication credentials, the name of the TSDB parent data container, and the path to the TSDB table within the container, must be defined as part of the CLI command or in a configuration file.")
	}
	if rc.logLevel != "" {
		cfg.LogLevel = rc.logLevel
	} else {
		cfg.LogLevel = config.DefaultLogLevel
	}

	rc.v3iocfg = cfg
	return nil
}

func (rc *RootCommandeer) startAdapter() error {
	var err error
	rc.adapter, err = tsdb.NewV3ioAdapter(rc.v3iocfg, nil, nil)
	if err != nil {
		return errors.Wrap(err, "Failed to start the TSDB Adapter.")
	}

	rc.logger = rc.adapter.GetLogger("cli")
	return nil
}
