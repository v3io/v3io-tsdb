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
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strconv"
	"time"
)

const (
	schemaVersion               = 0
	defaultStorageClass         = "local"
	defaultIngestionRate        = ""
	defaultRollupInterval       = "1h"
	defaultShardingBuckets      = 8
	defaultSampleRetentionHours = 0
	defaultLayerRetentionTime   = "1y"
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

	cmd.Flags().StringVarP(&commandeer.defaultRollups, "rollups", "r", "",
		"Default aggregation rollups, comma seperated: count,avg,sum,min,max,stddev")
	cmd.Flags().StringVarP(&commandeer.rollupInterval, "rollup-interval", "i", defaultRollupInterval, "aggregation interval")
	cmd.Flags().IntVarP(&commandeer.shardingBuckets, "sharding-buckets", "b", defaultShardingBuckets, "number of buckets to split key")
	cmd.Flags().IntVarP(&commandeer.sampleRetention, "sample-retention", "a", defaultSampleRetentionHours, "sample retention in hours")
	cmd.Flags().StringVar(&commandeer.sampleRate, "rate", defaultIngestionRate, "sample rate")

	commandeer.cmd = cmd

	return commandeer
}

func (cc *createCommandeer) create() error {

	// initialize params
	if err := cc.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := cc.validateRollupInterval(); err != nil {
		return errors.Wrap(err, "failed to parse rollup interval")
	}

	rollups, err := aggregate.AggregatorsToStringList(cc.defaultRollups)
	if err != nil {
		return errors.Wrap(err, "failed to parse default rollups")
	}

	if cc.sampleRate == "" {
		return errors.New(`sample rate not provided! Please provide sample rate with --rate flag in the format of [0-9]+/[hms]. Example: 12/m`)
	}
	rateInHours, err := rateToHours(cc.sampleRate)
	if err != nil {
		return errors.Wrap(err, "failed to parse sample rate")
	}

	chunkInterval, partitionInterval, err := cc.calculatePartitionAndChunkInterval(rateInHours)
	if err != nil {
		return errors.Wrap(err, "failed to calculate chunk interval")
	}

	defaultRollup := config.Rollup{
		Aggregators:            rollups,
		AggregatorsGranularity: cc.rollupInterval,
		StorageClass:           defaultStorageClass,
		SampleRetention:        cc.sampleRetention,
		LayerRetentionTime:     defaultLayerRetentionTime, //TODO: make configurable
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
		Partitions:          []*config.Partition{},
		Fields:              fields,
	}

	return tsdb.CreateTSDB(cc.rootCommandeer.v3iocfg, &schema)
}

func (cc *createCommandeer) validateRollupInterval() error {
	dayMillis := 24 * int64(time.Hour/time.Millisecond)
	duration, err := utils.Str2duration(cc.rollupInterval)
	if err != nil {
		return err
	}

	if dayMillis%duration != 0 && duration%dayMillis != 0 {
		return errors.New("rollup interval should be a divisor or a dividend of 1 day. Example: 10m, 30m, 2h, etc.")
	}
	return nil
}

func (cc *createCommandeer) calculatePartitionAndChunkInterval(rateInHours int) (string, string, error) {
	maxNumberOfEventsPerChunk := cc.rootCommandeer.v3iocfg.MaximumChunkSize / cc.rootCommandeer.v3iocfg.MaximumSampleSize
	minNumberOfEventsPerChunk := cc.rootCommandeer.v3iocfg.MinimumChunkSize / cc.rootCommandeer.v3iocfg.MaximumSampleSize

	chunkInterval := maxNumberOfEventsPerChunk / rateInHours
	if chunkInterval == 0 {
		return "", "", errors.New("sample rate is too high")
	}

	// Make sure the expected chunk size is greater then the supported minimum.
	if chunkInterval < minNumberOfEventsPerChunk/rateInHours {
		return "", "", fmt.Errorf(
			"calculated chunk size is less than minimum, rate - %v/h, calculated chunk interval - %v, minimum size - %v",
			rateInHours, chunkInterval, cc.rootCommandeer.v3iocfg.MinimumChunkSize)
	}

	actualCapacityOfChunk := chunkInterval * rateInHours * cc.rootCommandeer.v3iocfg.MaximumSampleSize
	numberOfChunksInPartition := 0

	for (numberOfChunksInPartition+24)*actualCapacityOfChunk < cc.rootCommandeer.v3iocfg.MaximumPartitionSize {
		numberOfChunksInPartition += 24
	}
	if numberOfChunksInPartition == 0 {
		return "", "", errors.Errorf("given rate is too high, can not fit a partition in a day interval with the calculated chunk size %vh", chunkInterval)
	}

	partitionInterval := numberOfChunksInPartition * chunkInterval
	return strconv.Itoa(chunkInterval) + "h", strconv.Itoa(partitionInterval) + "h", nil
}

func rateToHours(sampleRate string) (int, error) {
	parsingError := errors.New(`not a valid rate. Accepted pattern: [0-9]+/[hms]. Example: 12/m`)

	if len(sampleRate) < 3 {
		return 0, parsingError
	}
	if sampleRate[len(sampleRate)-2] != '/' {
		return 0, parsingError
	}

	last := sampleRate[len(sampleRate)-1]
	// get the number ignoring slash and time unit
	sampleRate = sampleRate[:len(sampleRate)-2]
	i, err := strconv.Atoi(sampleRate)
	if err != nil {
		return 0, errors.Wrap(err, parsingError.Error())
	}
	if i <= 0 {
		return 0, errors.New("rate should be a positive number")
	}
	switch last {
	case 's':
		return i * 60 * 60, nil
	case 'm':
		return i * 60, nil
	case 'h':
		return i, nil
	default:
		return 0, parsingError
	}
}
