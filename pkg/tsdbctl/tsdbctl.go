package tsdbctl

import (
	"os"

	"github.com/nuclio/nuclio/pkg/errors"

	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"strings"
	"github.com/nuclio/logger"
)

type RootCommandeer struct {
	adapter     *tsdb.V3ioAdapter
	logger      logger.Logger
	v3iocfg     *config.V3ioConfig
	cmd         *cobra.Command
	v3ioPath    string
	dbPath      string
	cfgFilePath string
	verbose     bool
}

func NewRootCommandeer() *RootCommandeer {
	commandeer := &RootCommandeer{}

	cmd := &cobra.Command{
		Use:   "tsdbctl [command]",
		Short: "V3IO TSDB command-line interface",
		//SilenceUsage:  true,
		//SilenceErrors: true,
	}

	defaultV3ioServer := os.Getenv("V3IO_SERVICE_URL")

	defaultCfgPath := os.Getenv("V3IO_FILE_PATH")
	if defaultCfgPath == "" {
		defaultCfgPath = "v3io.yaml"
	}

	cmd.PersistentFlags().BoolVarP(&commandeer.verbose, "verbose", "v", false, "Verbose output")
	cmd.PersistentFlags().StringVarP(&commandeer.dbPath, "dbpath", "p", "", "sub path for the TSDB, inside the container")
	cmd.PersistentFlags().StringVarP(&commandeer.v3ioPath, "server", "s", defaultV3ioServer, "V3IO Service URL - ip:port/container")
	cmd.PersistentFlags().StringVarP(&commandeer.cfgFilePath, "config", "c", defaultCfgPath, "path to yaml config file")

	// add children
	cmd.AddCommand(
		newAddCommandeer(commandeer).cmd,
		newQueryCommandeer(commandeer).cmd,
		newTimeCommandeer(commandeer).cmd,
		newCreateCommandeer(commandeer).cmd,
		newInfoCommandeer(commandeer).cmd,
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
	var err error
	cfg := &config.V3ioConfig{}

	if rc.cfgFilePath != "" {
		cfg, err = config.LoadConfig(rc.cfgFilePath)
		if err != nil {
			return errors.Wrap(err, "Failed to load config from file "+rc.cfgFilePath)
		}
	}

	if rc.v3ioPath != "" {
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
		return fmt.Errorf("User must provide V3IO URL, container name, and table path via the confif file or flags")
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
