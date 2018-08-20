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
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"strconv"
)

const schemaVersion = 0
const defaultStorageClass = "local"
const minimumSampleSize, maximumSampleSize = 2, 8     // bytes
const maximumPartitionSize = 2000000                  // 2MB
const minimumChunkSize, maximumChunkSize = 200, 62000 //bytes

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

	cmd.Flags().StringVarP(&commandeer.defaultRollups, "rollups", "r", "",
		"Default aggregation rollups, comma seperated: count,avg,sum,min,max,stddev")
	cmd.Flags().StringVarP(&commandeer.rollupInterval, "rollup-interval", "i", "1h", "aggregation interval")
	cmd.Flags().IntVarP(&commandeer.shardingBuckets, "sharding-buckets", "b", 8, "number of buckets to split key")
	cmd.Flags().IntVarP(&commandeer.sampleRetention, "sample-retention", "a", 0, "sample retention in hours")
	cmd.Flags().StringVarP(&commandeer.sampleRate, "sample-rate", "s", "12/m", "sample rate")

	commandeer.cmd = cmd

	return commandeer
}

func (cc *createCommandeer) create() error {

	// initialize params
	if err := cc.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := cc.validateFormat(cc.rollupInterval); err != nil {
		return errors.Wrap(err, "failed to parse rollup interval")
	}

	rollups, err := aggregate.AggregatorsToStringList(cc.defaultRollups)
	if err != nil {
		return errors.Wrap(err, "failed to parse default rollups")
	}

	chunkInterval, partitionInterval, err := cc.getPartitionAndChunkInterval()
	if err != nil {
		return errors.Wrap(err, "failed to parse sample rate")
	}

	defaultRollup := config.Rollup{
		Aggregators:            rollups,
		AggregatorsGranularity: cc.rollupInterval,
		StorageClass:           defaultStorageClass,
		SampleRetention:        cc.sampleRetention,
		LayerRetentionTime:     "1y", //TODO
	}

	tableSchema := config.TableSchema{
		Version:             schemaVersion,
		RollupLayers:        []config.Rollup{defaultRollup},
		ShardingBuckets:     cc.shardingBuckets,
		PartitionerInterval: partitionInterval,
		ChunckerInterval:    chunkInterval,
	}

	fields, err := aggregate.SchemaFieldFromString(rollups, "v")
	if err != nil {
		return errors.Wrap(err, "failed to create aggregators list")
	}
	fields = append(fields, config.SchemaField{Name: "_name", Type: "string", Nullable: false, Items: ""})

	partitionSchema := config.PartitionSchema{
		Version:                tableSchema.Version,
		Aggregators:            rollups,
		AggregatorsGranularity: cc.rollupInterval,
		StorageClass:           defaultStorageClass,
		SampleRetention:        cc.sampleRetention,
		ChunckerInterval:       tableSchema.ChunckerInterval,
		PartitionerInterval:    tableSchema.PartitionerInterval,
	}

	schema := config.Schema{
		TableSchemaInfo:     tableSchema,
		PartitionSchemaInfo: partitionSchema,
		Partitions:          []config.Partition{},
		Fields:              fields,
	}

	return tsdb.CreateTSDB(cc.rootCommandeer.v3iocfg, &schema)
}

func (cc *createCommandeer) validateFormat(format string) error {
	interval := format[0 : len(format)-1]
	if _, err := strconv.Atoi(interval); err != nil {
		return fmt.Errorf("format is inncorrect, not a number")
	}
	unit := string(format[len(format)-1])
	if !(unit == "m" || unit == "d" || unit == "h") {
		return fmt.Errorf("format is inncorrect, not part of m,d,h")
	}
	return nil
}

func (cc *createCommandeer) rateInHours() (int, error) {
	if len(cc.sampleRate) >= 3 {
		last := cc.sampleRate[len(cc.sampleRate)-1:]
		// get the number ignoring slash and time unit
		cc.sampleRate = cc.sampleRate[0 : len(cc.sampleRate)-2]
		i, err := strconv.Atoi(cc.sampleRate)
		if err != nil {
			return 0, errors.Wrap(err, `not a valid rate. Accepted pattern: [0-9]+/[dhms]. Examples: 12/m`)
		}
		switch last {
		case "s":
			return i * 60 * 60, nil
		case "m":
			return i * 60, nil
		case "h":
			return i, nil
		case "d":
			return i / 24, nil
		default:
			return 0, fmt.Errorf(`not a valid rate. Accepted pattern: [0-9]+/[dhms]. Examples: 12/m`)
		}
	}

	return 0, fmt.Errorf(`not a valid rate. Accepted pattern: [0-9]+/[dhms]. Examples: 12/m`)
}

// This method calculates the chunk and partition interval from the given sample rate.
func (cc *createCommandeer) getPartitionAndChunkInterval() (string, string, error) {
	maxNumberOfEventsPerChunk := maximumChunkSize / maximumSampleSize
	minNumberOfEventsPerChunk := minimumChunkSize / minimumSampleSize

	expectedRateInHours, err := cc.rateInHours()
	if err != nil {
		return "", "", errors.Wrap(err, "failed to parse sample rate")
	}

	chunkInterval := maxNumberOfEventsPerChunk / expectedRateInHours

	// Make sure the expected chunk size is greater then the supported minimum.
	for chunkInterval*expectedRateInHours < minNumberOfEventsPerChunk {
		chunkInterval++
	}

	actualCapacityOfChunk := chunkInterval * expectedRateInHours * maximumSampleSize
	numberOfChunksInPartition := maximumPartitionSize / actualCapacityOfChunk
	partitionInterval := numberOfChunksInPartition * chunkInterval
	return strconv.Itoa(chunkInterval) + "h", strconv.Itoa(partitionInterval) + "h", nil
}
