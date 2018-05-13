package tsdbctl

import (
	"github.com/pkg/errors"
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
		Use:   "create path",
		Short: "create a new TSDB in the specifies path",
		RunE: func(cmd *cobra.Command, args []string) error {

			// if we got positional arguments
			if len(args) != 1 {
				return errors.New("create require TSDB path")
			}

			commandeer.path = args[0]

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

	v3iocfg, err := config.LoadV3ioConfig(cc.rootCommandeer.cfgPath)
	if err != nil {
		return errors.Wrap(err, "Failed to load config")
	}

	dbcfg := config.DBPartConfig{
		DaysPerObj:     cc.daysPerObj,
		HrInChunk:      cc.hrInChunk,
		DefaultRollups: cc.defaultRollups,
		RollupMin:      cc.rollupMin,
	}

	return tsdb.CreateTSDB(v3iocfg, cc.path, &dbcfg)

}
