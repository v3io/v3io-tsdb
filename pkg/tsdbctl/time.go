package tsdbctl

import (
	"fmt"
	"github.com/spf13/cobra"
	"strconv"
	"time"
)

type timeCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
}

func newTimeCommandeer(rootCommandeer *RootCommandeer) *timeCommandeer {
	commandeer := &timeCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "time [unix-time | time-RFC3339]",
		Aliases: []string{"put"},
		Short:   "returns current unix time or the unix/RFC3339 matching the provided RFC3339/unix",
		RunE: func(cmd *cobra.Command, args []string) error {

			// if we got positional arguments
			if len(args) == 0 {
				fmt.Println(time.Now().Unix())
				return nil
			}

			tint, err := strconv.Atoi(args[0])
			if err == nil {
				fmt.Println(time.Unix(int64(tint), 0).Format(time.RFC3339))
				return nil
			}

			t, err := time.Parse(time.RFC3339, args[0])

			if err == nil {
				fmt.Println(t.Unix())
			}

			return err

		},
	}

	commandeer.cmd = cmd

	return commandeer
}
