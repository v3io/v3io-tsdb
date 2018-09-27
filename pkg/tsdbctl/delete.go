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
		Use:     "del",
		Short:   "delete a TSDB",
		Aliases: []string{"delete"},
		RunE: func(cmd *cobra.Command, args []string) error {

			// initialize params
			return commandeer.delete()
		},
	}

	cmd.Flags().BoolVarP(&commandeer.deleteAll, "all", "a", false, "Delete the TSDB table (including all content and the configuration schema file)")
	cmd.Flags().BoolVarP(&commandeer.ignoreErrors, "ignore-errors", "i", false, "Delete all elements even if some steps fail")
	cmd.Flags().BoolVarP(&commandeer.force, "force", "f", false, "Delete without prompt")
	cmd.Flags().StringVarP(&commandeer.toTime, "end", "e", "now", "TO time")
	cmd.Flags().StringVarP(&commandeer.fromTime, "begin", "b", "0", "FROM time")
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

	var err error
	to := time.Now().Unix() * 1000
	if ic.toTime != "" {
		to, err = utils.Str2unixTime(ic.toTime)
		if err != nil {
			return err
		}
	}
	from := to - 1000*3600 // default of last hour
	if ic.fromTime != "" {
		from, err = utils.Str2unixTime(ic.fromTime)
		if err != nil {
			return err
		}
	}

	if !ic.force {
		confirmedByUser, err := getConfirmation(
			fmt.Sprintf("You are about to delete the '%s' table. Are you sure?", ic.rootCommandeer.v3iocfg.TablePath))
		if err != nil {
			return err
		}

		if !confirmedByUser {
			return errors.New("Cancelled by user")
		}
	}

	err = ic.rootCommandeer.adapter.DeleteDB(ic.deleteAll, ic.ignoreErrors, from, to)
	if err != nil {
		return errors.Wrapf(err, "Failed to delete table '%s'", ic.rootCommandeer.v3iocfg.TablePath)
	}
	fmt.Printf("Table '%s' has been deleted\n", ic.rootCommandeer.v3iocfg.TablePath)

	return nil
}

func getConfirmation(prompt string) (bool, error) {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%s [y/n]: ", prompt)

		response, err := reader.ReadString('\n')
		if err != nil {
			errors.Wrap(err, "failed to get user input")
		}

		response = strings.ToLower(strings.TrimSpace(response))

		if response == "y" || response == "yes" {
			return true, nil
		} else if response == "n" || response == "no" {
			return false, nil
		}
	}
}
