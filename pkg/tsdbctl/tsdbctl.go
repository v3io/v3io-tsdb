package tsdbctl

import (
	"os"

	"github.com/nuclio/nuclio/pkg/errors"

	"github.com/nuclio/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type RootCommandeer struct {
	loggerInstance logger.Logger
	cmd            *cobra.Command
	v3io           string
	dbpath         string
	cfgPath        string
	verbose        bool
}

func NewRootCommandeer() *RootCommandeer {
	commandeer := &RootCommandeer{}

	cmd := &cobra.Command{
		Use:           "tsdbctl [command]",
		Short:         "V3IO TSDB command-line interface",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	defaultV3ioServer := os.Getenv("V3IO_SERVICE_URL")

	defaultCfgPath := os.Getenv("V3IO_FILE_PATH")
	if defaultCfgPath == "" {
		defaultCfgPath = "v3io.yaml"
	}

	cmd.PersistentFlags().BoolVarP(&commandeer.verbose, "verbose", "v", false, "Verbose output")
	cmd.PersistentFlags().StringVarP(&commandeer.dbpath, "dbpath", "p", "metrics", "sub path for the TSDB, inside the container")
	cmd.PersistentFlags().StringVarP(&commandeer.v3io, "server", "s", defaultV3ioServer, "V3IO Service URL - ip:port/container")
	cmd.PersistentFlags().StringVarP(&commandeer.cfgPath, "config", "c", defaultCfgPath, "path to yaml config file")

	// add children
	cmd.AddCommand(
		newAddCommandeer(commandeer).cmd,
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

	rc.loggerInstance, err = utils.NewLogger(rc.verbose)
	if err != nil {
		return errors.Wrap(err, "Failed to create logger")
	}

	return nil
}
