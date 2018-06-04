package tsdbctl

import (
	"github.com/spf13/cobra"
	"github.com/pkg/errors"
)

type delCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	delConfig      bool
	force          bool
}

func newDeleteCommandeer(rootCommandeer *RootCommandeer) *delCommandeer {
	commandeer := &delCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "del",
		Short: "delete a TSDB",
		RunE: func(cmd *cobra.Command, args []string) error {

			// initialize params
			return commandeer.delete()
		},
	}

	cmd.Flags().BoolVarP(&commandeer.delConfig, "del-config", "d", false, "Delete the TSDB config as well")
	cmd.Flags().BoolVarP(&commandeer.force, "force", "f", false, "Delete all elements even if some steps fail")
	commandeer.cmd = cmd

	return commandeer
}

func (ic *delCommandeer) delete() error {

	if err := ic.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := ic.rootCommandeer.startAdapter(); err != nil {
		return err
	}

	err := ic.rootCommandeer.adapter.DeleteDB(ic.delConfig, ic.force)
	if err != nil {
		return errors.Wrap(err, "Failed to delete DB")
	}

	return nil
}

