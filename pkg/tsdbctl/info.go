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
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

type infoCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	getNames       bool
	getCount       bool
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
			return commandeer.info()
		},
	}

	cmd.Flags().BoolVarP(&commandeer.getNames, "names", "n", false, "return metric names")
	cmd.Flags().BoolVarP(&commandeer.getCount, "performance", "m", false, "count number metric objects")

	commandeer.cmd = cmd

	return commandeer
}

func (ic *infoCommandeer) info() error {

	if err := ic.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := ic.rootCommandeer.startAdapter(); err != nil {
		return err
	}

	dbconfig := ic.rootCommandeer.adapter.GetSchema()
	info, err := yaml.Marshal(dbconfig)
	if err != nil {
		return errors.Wrap(err, "Failed to get config")
	}

	fmt.Printf("TSDB Table %s Configuration:\n", ic.rootCommandeer.v3iocfg.TablePath)
	fmt.Println(string(info))

	if ic.getNames {
		// create a querier
		qry, err := ic.rootCommandeer.adapter.Querier(nil, 0, 0)
		if err != nil {
			return errors.Wrap(err, "Failed to create querier")
		}

		// get all metric names
		names, err := qry.LabelValues("__name__")
		if err != nil {
			return errors.Wrap(err, "Failed to get labels")
		}

		fmt.Println("Metric Names:")
		for _, name := range names {
			fmt.Println(name)
		}
	}

	if ic.getCount {
		count, err := ic.rootCommandeer.adapter.CountMetrics("")
		if err != nil {
			return errors.Wrap(err, "Failed to count")
		}

		fmt.Println("Number of objects: ", count)
	}

	return nil
}
