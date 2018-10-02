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
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
)

type createCommandeer struct {
	cmd             *cobra.Command
	rootCommandeer  *RootCommandeer
	path            string
	storageClass    string
	defaultRollups  string
	rollupInterval  string
	shardingBuckets int
	sampleRetention int
	sampleRate      string
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

	cmd.Flags().StringVarP(&commandeer.defaultRollups, "aggregates", "a", "",
		"Default aggregation rollups, comma seperated: count,avg,sum,min,max,stddev")
	cmd.Flags().StringVarP(&commandeer.rollupInterval, "aggregation-granularity", "i", config.DefaultAggregationGranularity, "aggregation interval")
	cmd.Flags().IntVarP(&commandeer.shardingBuckets, "sharding-buckets", "b", config.DefaultShardingBuckets, "number of buckets to split key")
	cmd.Flags().StringVarP(&commandeer.sampleRate, "ingestion-rate", "r", config.DefaultIngestionRate, "Metric-samples ingestion rate")

	commandeer.cmd = cmd

	return commandeer
}

func (cc *createCommandeer) create() error {

	// initialize params
	if err := cc.rootCommandeer.initialize(); err != nil {
		return err
	}

	dbSchema, err := schema.NewSchema(
		cc.rootCommandeer.v3iocfg,
		cc.sampleRate,
		cc.rollupInterval,
		cc.defaultRollups)

	if err != nil {
		return errors.Wrap(err, "failed to create TSDB schema")
	}

	return tsdb.CreateTSDB(cc.rootCommandeer.v3iocfg, dbSchema)
}
