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
	getNames       bool
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

			return commandeer.info()
		},
	}

	cmd.Flags().BoolVarP(&commandeer.getNames, "names", "n", false, "return metric names")

	commandeer.cmd = cmd

	return commandeer
}

func (ic *infoCommandeer) info() error {
	dbconfig := ic.rootCommandeer.adapter.GetDBConfig()
	info, err := yaml.Marshal(dbconfig)
	if err != nil {
		return errors.Wrap(err, "Failed to get config")
	}

	fmt.Println("TSDB Configuration:")
	fmt.Println(string(info))

	if ic.getNames {
		// create a querier
		qry, err := ic.rootCommandeer.adapter.Querier(nil, 0, 0)
		if err != nil {
			return errors.Wrap(err, "Failed to create querier")
		}

		// get all metric names
		names, err := qry.LabelValues("")
		if err != nil {
			return errors.Wrap(err, "Failed to get labels")
		}

		fmt.Println("Metric Names:")
		for _, name := range names {
			fmt.Println(name)
		}
	}

	return nil
}
