package tsdbctl

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

type addCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	lset           string
	tArr           string
	vArr           string
}

func newAddCommandeer(rootCommandeer *RootCommandeer) *addCommandeer {
	commandeer := &addCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "add metric",
		Aliases: []string{"proj"},
		Short:   "add samples to metric",
		RunE: func(cmd *cobra.Command, args []string) error {

			// if we got positional arguments
			if len(args) != 1 {
				return errors.New("Project create requires an identifier")
			}

			commandeer.lset = args[0]

			// initialize root
			if err := rootCommandeer.initialize(); err != nil {
				return errors.Wrap(err, "Failed to initialize root")
			}

			return nil

		},
	}

	cmd.Flags().StringVarP(&commandeer.tArr, "times", "t", "", "time array, comma separated")
	cmd.Flags().StringVarP(&commandeer.vArr, "values", "d", "", "values array, comma separated")

	commandeer.cmd = cmd

	return commandeer
}
