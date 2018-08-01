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
	"strings"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"strconv"
	"fmt"
)

const SCHEMA_VERSION = 0
const DEFAULT_STORAGE_CLASS = "local"

type createCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	path           string
	partitionInterval string
	storageClass string
	chunkInterval string
	defaultRollups string
	rollupInterval int
	shardingBuckets int
	sampleRetention int
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

	cmd.Flags().StringVarP(&commandeer.partitionInterval, "partition-interval", "p", "1D", "time covered per partition")
	cmd.Flags().StringVarP(&commandeer.chunkInterval, "chunk-interval", "t", "1H", "time in a single chunk")
	cmd.Flags().StringVarP(&commandeer.defaultRollups, "rollups", "r", "",
		"Default aggregation rollups, comma seperated: count,avg,sum,min,max,stddev")
	cmd.Flags().IntVarP(&commandeer.rollupInterval, "rollup-interval", "i", 3600, "aggregation interval in seconds")
	cmd.Flags().IntVarP(&commandeer.shardingBuckets, "sharding-buckets", "b", 64, "number of buckets to split key")
	cmd.Flags().IntVarP(&commandeer.shardingBuckets, "sample-retention", "s", 0, "nsample retention in hours")

	commandeer.cmd = cmd

	return commandeer
}

func (cc *createCommandeer) create() error {

	// initialize params
	if err := cc.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := cc.validateFormat(cc.partitionInterval); err != nil {
		return errors.Wrap(err, "Failed to parse partition interval")
	}

	if err := cc.validateFormat(cc.chunkInterval); err != nil {
		return errors.Wrap(err, "Failed to parse chunk interval")
	}


	defaultRollup := config.Rollup {
		Aggregators: cc.defaultRollups,
		AggregatorsGranularityInSeconds: cc.rollupInterval,
		StorageClass: DEFAULT_STORAGE_CLASS,
		SampleRetention: cc.sampleRetention,
		LayerRetentionTime: "1Y", //TODO
	}

	tableSchema := config.TableSchema{
		Version: SCHEMA_VERSION,
		RollupLayers: []config.Rollup{defaultRollup},
		ShardingBuckets: cc.shardingBuckets,
		PartitionerInterval: cc.partitionInterval,
		ChunckerInterval: cc.chunkInterval,
	}

	aggrs := strings.Split(cc.defaultRollups, ",")
	fields, err := aggregate.SchemaFieldFromString(aggrs)
	if err != nil {
		return errors.Wrap(err, "Failed to create aggregators list")
	}
	fields = append(fields, config.SchemaField{Name: "_name", Type: "string", Nullable: false, Items: ""})
	fields = append(fields, config.SchemaField{Name: "partId", Type: "int", Nullable: false, Items: ""})

	partitionSchema := config.PartitionSchema{
		Version: tableSchema.Version,
		Aggregators: aggrs,
		AggregatorsGranularityInSeconds: cc.rollupInterval,
		StorageClass: DEFAULT_STORAGE_CLASS,
		SampleRetention: cc.sampleRetention,
		ChunckerInterval: tableSchema.ChunckerInterval,
		PartitionerInterval: tableSchema.PartitionerInterval,
	}

	schema := config.Schema{
		TableSchemaInfo: tableSchema,
		PartitionSchemaInfo: partitionSchema,
		Partitions: []config.Partition{},
		Fields: fields,
	}

	return tsdb.CreateTSDB(cc.rootCommandeer.v3iocfg, &schema)

}

func (cc *createCommandeer) validateFormat(format string) error {
	interval := format[0]
	if _, err := strconv.Atoi(string(interval)); err != nil {
		return fmt.Errorf("format is inncorrect, not a number")
	}
	date := format[1]
	if ! (date == 'Y' || date == 'M' || date == 'D' || date == 'H' || date == 'm') {
		return fmt.Errorf("format is inncorrect, not part of Y/M/D/H/m")
	}
	return nil
}