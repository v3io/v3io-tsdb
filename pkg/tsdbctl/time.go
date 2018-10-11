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
		Use:    "time [<time>]",
		Hidden: true,
		Short:  "Performs RFC3339 time/Unix timestamp conversions",
		Long: `Converts an RFC3339 time string to a Unix timestamp in seconds, or vice versa.
By default, returns the current time as a Unix timestamp.`,
		Example: `- tsdbctl time
- tsdcbtl time 2016-01-02T15:34:26Z
- tsdcbtl time 1537971020

Notes:
- The global flags are not applicable to this command.

Arguments:
  <time> (string)  An RFC3339 time string or a Unix timestamp in seconds.
                   By default, the command returns the Unix timestamp for the current time.`,
		RunE: func(cmd *cobra.Command, args []string) error {

			// Check for positional arguments
			if len(args) == 0 {
				fmt.Println(time.Now().Unix())
				return nil
			}

			tint, err := strconv.Atoi(args[0])
			if err == nil {
				fmt.Println(time.Unix(int64(tint), 0).UTC().Format(time.RFC3339))
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
