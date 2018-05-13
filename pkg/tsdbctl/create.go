package tsdbctl

import (
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-tsdb/config"
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

			// initialize params
			if err := rootCommandeer.initialize(); err != nil {
				return err
			}

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

	dbcfg := config.DBPartConfig{
		DaysPerObj:     cc.daysPerObj,
		HrInChunk:      cc.hrInChunk,
		DefaultRollups: cc.defaultRollups,
		RollupMin:      cc.rollupMin,
	}

	return tsdb.CreateTSDB(cc.rootCommandeer.v3iocfg, &dbcfg)

}
