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
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
)

type RootCommandeer struct {
	adapter     *tsdb.V3ioAdapter
	logger      logger.Logger
	v3iocfg     *config.V3ioConfig
	cmd         *cobra.Command
	v3ioUrl     string
	dbPath      string
	cfgFilePath string
	logLevel    string
	container   string
	username    string
	password    string
	accessKey   string
	Reporter    *performance.MetricReporter
	BuildInfo   *config.BuildInfo
}

func NewRootCommandeer() *RootCommandeer {
	commandeer := &RootCommandeer{
		BuildInfo: config.BuildMetadta,
	}

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
	cmd.PersistentFlags().StringVarP(&commandeer.v3ioUrl, "server", "s", defaultV3ioServer,
		"Web-gateway (web-APIs) service endpoint of an instance of\nthe Iguazio Continuous Data Platform, of the format\n\"<IP address>:<port number=8081>\". Examples: \"localhost:8081\"\n(when running on the target platform); \"192.168.1.100:8081\".")
	cmd.PersistentFlags().StringVarP(&commandeer.cfgFilePath, "config", "g", "",
		"Path to a YAML TSDB configuration file. When this flag isn't\nset, the CLI checks for a "+config.DefaultConfigurationFileName+" configuration\nfile in the current directory. CLI flags override file\nconfigurations. Example: \"~/cfg/my_v3io_tsdb_cfg.yaml\".")
	cmd.PersistentFlags().StringVarP(&commandeer.container, "container", "c", "",
		"The name of an Iguazio Continuous Data Platform data container\nin which to create the TSDB table. Example: \"bigdata\".")
	cmd.PersistentFlags().StringVarP(&commandeer.username, "username", "u", "",
		"Username of an Iguazio Continuous Data Platform user.")
	cmd.PersistentFlags().StringVarP(&commandeer.password, "password", "p", "",
		"Password of the configured user (see -u|--username).")
	cmd.PersistentFlags().StringVarP(&commandeer.accessKey, "access-key", "k", "",
		"Access-key for accessing the required table.\nIf access-key is passed, it will take precedence on user/password authentication.")

	// Add children
	cmd.AddCommand(
		newAddCommandeer(commandeer).cmd,
		newQueryCommandeer(commandeer).cmd,
		newTimeCommandeer(commandeer).cmd,
		newCreateCommandeer(commandeer).cmd,
		newInfoCommandeer(commandeer).cmd,
		newDeleteCommandeer(commandeer).cmd,
		newCheckCommandeer(commandeer).cmd,
		newVersionCommandeer(commandeer).cmd,
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

	if rc.accessKey != "" {
		cfg.AccessKey = rc.accessKey
	} else if rc.password == "" {
		envAccessKey := os.Getenv("V3IO_ACCESS_KEY")
		if envAccessKey != "" {
			cfg.AccessKey = envAccessKey
		}
	}

	envV3ioApi := os.Getenv("V3IO_API")
	if envV3ioApi != "" {
		cfg.WebApiEndpoint = envV3ioApi
	}
	if rc.v3ioUrl != "" {
		cfg.WebApiEndpoint = rc.v3ioUrl
	}
	if rc.container != "" {
		cfg.Container = rc.container
	}
	if rc.dbPath != "" {
		cfg.TablePath = rc.dbPath
	}
	if cfg.WebApiEndpoint == "" {
		return errors.New("web API endpoint must be set")
	}
	if cfg.Container == "" {
		return errors.New("container must be set")
	}
	if cfg.TablePath == "" {
		return errors.New("table path must be set")
	}
	if rc.logLevel != "" {
		cfg.LogLevel = rc.logLevel
	} else {
		cfg.LogLevel = config.DefaultLogLevel
	}

	// Prefix http:// in case that WebApiEndpoint is a pseudo-URL missing a scheme (for backward compatibility).
	amendedWebApiEndpoint, err := buildUrl(cfg.WebApiEndpoint)
	if err == nil {
		cfg.WebApiEndpoint = amendedWebApiEndpoint
	}

	rc.v3iocfg = cfg
	return nil
}

func buildUrl(webApiEndpoint string) (string, error) {
	if !strings.HasPrefix(webApiEndpoint, "http://") && !strings.HasPrefix(webApiEndpoint, "https://") {
		webApiEndpoint = "http://" + webApiEndpoint
	}
	endpointUrl, err := url.Parse(webApiEndpoint)
	if err != nil {
		return "", err
	}
	endpointUrl.Path = ""
	return endpointUrl.String(), nil
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
