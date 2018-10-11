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
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"os"
	"strings"
	"time"
)

type delCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	deleteAll      bool
	ignoreErrors   bool
	force          bool
	fromTime       string
	toTime         string
}

func newDeleteCommandeer(rootCommandeer *RootCommandeer) *delCommandeer {
	commandeer := &delCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Aliases: []string{"delete"},
		Use:     "del",
		Short:   "Delete a TSDB instance or its content",
		Long:    `Delete a TSDB instance (table) or delete content from the table.`,
		Example: `The examples assume that the endpoint of the web-gateway service, the login credentials, and
the name of the data container are configured in the default configuration file (` + config.DefaultConfigurationFileName + `)
instead of using the -s|--server, -u|--username, -p|--password, and -c|--container flags.
- tsdbctl delete -t metrics_tsdb -a
- tsdbctl delete -t dbs/perfstats -f
- tsdbctl delete -t my_tsdb -b 0 -e now-7d -i

Notes:
- When deleting content within a specific time range (see the -b|--begin and -e|--end flags and
  their default values), all partitions containing data within this range are deleted, including
  metric items with older or newer times. Use the info command to view the partitioning interval.`,
		RunE: func(cmd *cobra.Command, args []string) error {

			// Initialize parameters
			return commandeer.delete()
		},
	}

	cmd.Flags().BoolVarP(&commandeer.deleteAll, "all", "a", false,
		"Delete the TSDB table, including its configuration and all content.")
	cmd.Flags().BoolVarP(&commandeer.ignoreErrors, "ignore-errors", "i", false,
		"Ignore errors - continue deleting even if some steps fail.")
	cmd.Flags().BoolVarP(&commandeer.force, "force", "f", false,
		"Forceful deletion - don't display a delete-verification prompt.")
	cmd.Flags().StringVarP(&commandeer.toTime, "end", "e", "",
		"End (maximum) time for the delete operation, as a string containing an\nRFC3339 time string, a Unix timestamp in milliseconds, or a relative\ntime of the format \"now\" or \"now-[0-9]+[mhd]\" (where 'm' = minutes,\n'h' = hours, and 'd' = days). Examples: \"2018-09-26T14:10:20Z\";\n\"1537971006000\"; \"now-3h\"; \"now-7d\". (default \"now\")")
	cmd.Flags().StringVarP(&commandeer.fromTime, "begin", "b", "",
		"Start (minimum) time for the delete operation, as a string containing\nan RFC3339 time, a Unix timestamp in milliseconds, a relative time of\nthe format \"now\" or \"now-[0-9]+[mhd]\" (where 'm' = minutes, 'h' = hours,\nand 'd' = days), or 0 for the earliest time. Examples:\n\"2016-01-02T15:34:26Z\"; \"1451748866\"; \"now-90m\"; \"0\". (default =\n<end time> - 1h)")
	commandeer.cmd = cmd

	return commandeer
}

func (dc *delCommandeer) delete() error {

	if err := dc.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := dc.rootCommandeer.startAdapter(); err != nil {
		return err
	}

	var err error
	to := time.Now().Unix() * 1000
	if dc.toTime != "" {
		to, err = utils.Str2unixTime(dc.toTime)
		if err != nil {
			return err
		}
	}
	from := to - 1000*3600 // Default start time = one hour before the end time
	if dc.fromTime != "" {
		from, err = utils.Str2unixTime(dc.fromTime)
		if err != nil {
			return err
		}
	}

	if !dc.force {
		confirmedByUser, err := getConfirmation(
			fmt.Sprintf("You are about to delete TSDB table '%s' in container '%s'. Are you sure?", dc.rootCommandeer.v3iocfg.TablePath, dc.rootCommandeer.v3iocfg.Container))
		if err != nil {
			return err
		}

		if !confirmedByUser {
			return errors.New("Delete cancelled by the user.")
		}
	}

	err = dc.rootCommandeer.adapter.DeleteDB(dc.deleteAll, dc.ignoreErrors, from, to)
	if err != nil {
		return errors.Wrapf(err, "Failed to delete TSDB table '%s' in container '%s'.", dc.rootCommandeer.v3iocfg.TablePath, dc.rootCommandeer.v3iocfg.Container)
	}
	fmt.Printf("Successfully deleted TSDB table '%s' from container '%s'.\n", dc.rootCommandeer.v3iocfg.TablePath, dc.rootCommandeer.v3iocfg.Container)

	return nil
}

func getConfirmation(prompt string) (bool, error) {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%s [y/n]: ", prompt)

		response, err := reader.ReadString('\n')
		if err != nil {
			errors.Wrap(err, "Failed to get user input.")
		}

		response = strings.ToLower(strings.TrimSpace(response))

		if response == "y" || response == "yes" {
			return true, nil
		} else if response == "n" || response == "no" {
			return false, nil
		}
	}
}
