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
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
)

type createCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	path           string
	daysPerObj     int
	hrInChunk      int
	defaultRollups string
	rollupMin      int
}

func newCreateCommandeer(rootCommandeer *RootCommandeer) *createCommandeer {
	commandeer := &createCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a new TSDB in the specifies path",
		RunE: func(cmd *cobra.Command, args []string) error {

			return commandeer.create()

		},
	}

	cmd.Flags().IntVarP(&commandeer.daysPerObj, "days", "d", 1, "number of days covered per partition")
	cmd.Flags().IntVarP(&commandeer.hrInChunk, "chunk-hours", "t", 1, "number of hours in a single chunk")
	cmd.Flags().StringVarP(&commandeer.defaultRollups, "rollups", "r", "",
		"Default aggregation rollups, comma seperated: count,avg,sum,min,max,stddev")
	cmd.Flags().IntVarP(&commandeer.rollupMin, "rollup-interval", "i", 60, "aggregation interval in minutes")

	commandeer.cmd = cmd

	return commandeer
}

func (cc *createCommandeer) create() error {

	// initialize params
	if err := cc.rootCommandeer.initialize(); err != nil {
		return err
	}

	dbcfg := config.DBPartConfig{
		DaysPerObj:     cc.daysPerObj,
		HrInChunk:      cc.hrInChunk,
		DefaultRollups: cc.defaultRollups,
		RollupMin:      cc.rollupMin,
	}

	return tsdb.CreateTSDB(cc.rootCommandeer.v3iocfg, &dbcfg)

}
