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
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"time"
)

type delCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	delConfig      bool
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

	cmd.Flags().BoolVarP(&commandeer.delConfig, "del-config", "d", false, "Delete the TSDB config as well")
	cmd.Flags().BoolVarP(&commandeer.force, "force", "f", false, "Delete all elements even if some steps fail")
	cmd.Flags().StringVarP(&commandeer.toTime, "end", "e", "now", "to time")
	cmd.Flags().StringVarP(&commandeer.fromTime, "begin", "b", "0", "from time")
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
	err = ic.rootCommandeer.adapter.DeleteDB(ic.delConfig, ic.force, from, to)
	if err != nil {
		return errors.Wrap(err, "Failed to delete DB")
	}
	fmt.Printf("Deleted table %s succsesfuly\n", ic.rootCommandeer.v3iocfg.Path)

	return nil
}
