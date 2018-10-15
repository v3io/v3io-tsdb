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
		Short: "Display information about a TSDB instance",
		Long:  `Display configuration and metrics information for a TSDB instance (table).`,
		Example: `- tsdbctl info -s 192.168.1.100:8081 -u myuser -p mypassword -c bigdata -t mytsdb -m -n
- tsdbctl info -s 192.168.1.100:8081 -u jerrys -p OpenSesame -c mycontainer -t my_tsdb`,
		RunE: func(cmd *cobra.Command, args []string) error {

			// Initialize parameters
			return commandeer.info()
		},
	}

	cmd.Flags().BoolVarP(&commandeer.getNames, "names", "n", false,
		"Display the metric names in the TSDB.")
	cmd.Flags().BoolVarP(&commandeer.getCount, "performance", "m", false,
		"Display the number of metric items in the TSDB.")

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
		return errors.Wrap(err, "Failed to get TSDB configuration.")
	}

	fmt.Printf("TSDB configuration for table '%s':\n", ic.rootCommandeer.v3iocfg.TablePath)
	fmt.Println(string(info))

	if ic.getNames {
		// Create a Querier
		qry, err := ic.rootCommandeer.adapter.Querier(nil, 0, 0)
		if err != nil {
			return errors.Wrap(err, "Failed to create a Querier object.")
		}

		// Get all metric names
		names, err := qry.LabelValues("__name__")
		if err != nil {
			return errors.Wrap(err, "Failed to get the metric names.")
		}

		fmt.Println("Metric names:")
		for _, name := range names {
			fmt.Println(name)
		}
	}

	if ic.getCount {
		count, err := ic.rootCommandeer.adapter.CountMetrics("")
		if err != nil {
			return errors.Wrap(err, "Failed to count the metric items.")
		}

		fmt.Println("Number of metric items: ", count)
	}

	return nil
}
