package tsdbctl

import (
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

type infoCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
}

func newInfoCommandeer(rootCommandeer *RootCommandeer) *infoCommandeer {
	commandeer := &infoCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "info",
		Short: "return TSDB config",
		RunE: func(cmd *cobra.Command, args []string) error {

			// initialize params
			if err := rootCommandeer.initialize(); err != nil {
				return err
			}

			if err := rootCommandeer.startAdapter(); err != nil {
				return err
			}

			dbconfig := rootCommandeer.adapter.GetDBConfig()
			info, err := yaml.Marshal(dbconfig)
			if err != nil {
				return errors.Wrap(err, "Failed to get config")
			}

			fmt.Println(string(info))

			return nil
		},
	}

	commandeer.cmd = cmd

	return commandeer
}
