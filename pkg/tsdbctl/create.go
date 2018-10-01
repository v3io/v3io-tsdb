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
	schemaVersion          = 0
	defaultStorageClass    = "local"
	defaultIngestionRate   = ""
	defaultRollupInterval  = "1h"
	defaultShardingBuckets = 8
	// TODO: enable sample-retention when supported
	// defaultSampleRetentionHours = 0
	defaultLayerRetentionTime = "1y"
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
		Short: "Create a new TSDB instance",
		Long:  `Create a new TSDB instance (table) according to the provided configuration.`,
		Example: `- tsdbctl create -s 192.168.1.100:8081 -u myuser -p mypassword -c mycontainer -t my_tsdb --rate 1/s
- tsdbctl create -s 192.168.204.14:8081 -u janed -p OpenSesame -c bigdata -t my_dbs/metrics_table --rate 60/m -a "min,avg,stddev" -i 3h
- tsdbctl create -g ~/my_tsdb_cfg.yaml -u johnl -p "P@ssNoW!" -c admin_container -t perf_metrics --rate "100/h"
  (where ~/my_tsdb_cfg.yaml sets "webApiEndpoint" to the endpoint of the web-gateway service)`,
		RunE: func(cmd *cobra.Command, args []string) error {

			return commandeer.create()

		},
	}

	cmd.Flags().StringVarP(&commandeer.defaultRollups, "aggregates", "a", "",
		"Default aggregates to calculate in real time during\nthe samples ingestion, as a comma-separated list of\nsupported aggregation functions - count | avg | sum |\nmin | max | stddev | stdvar | last | rate.\nExample: \"sum,avg,max\".")
	cmd.Flags().StringVarP(&commandeer.rollupInterval, "aggregation-granularity", "i", defaultRollupInterval,
		"Aggregation granularity - a time interval for applying\nthe aggregation functions (if  configured - see the\n-a|--aggregates flag), of the format \"[0-9]+[mh]\"\n(where 'm' = minutes and 'h' = hours).\nExamples: \"2h\"; \"90m\".")
	cmd.Flags().IntVarP(&commandeer.shardingBuckets, "sharding-buckets", "b", defaultShardingBuckets,
		"Number of storage buckets across which to split the\ndata of a single metric to optimize storage of\nnon-uniform data. Example: 10.")
	// TODO: enable sample-retention when supported:
	// cmd.Flags().IntVarP(&commandeer.sampleRetention, "sample-retention", "r", defaultSampleRetentionHours,
	//	"Metric-samples retention period, in hours. Example: 1 (retain samples for 1 hour).")
	cmd.Flags().StringVarP(&commandeer.sampleRate, "rate", "r", defaultIngestionRate,
		"[Required] Metric-samples ingestion rate - the maximum\ningestion rate for a single metric (calculated\naccording to the slowest expecetd ingestion rate) -\nof the format \"[0-9]+/[mhd]\" (where 'm' = minutes,\n'h' = hours, and 'd' = days). Examples: \"12/m\" (12\nsamples per minute); \"1s\" (one sample per second).")

	commandeer.cmd = cmd

	return commandeer
}

func (cc *createCommandeer) create() error {

	// initialize params
	if err := cc.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err := cc.validateRollupInterval(); err != nil {
		return errors.Wrap(err, "Failed to parse the aggregation granularity.")
	}

	rollups, err := aggregate.AggregatorsToStringList(cc.defaultRollups)
	if err != nil {
		return errors.Wrap(err, "Failed to parse the default-aggregates list.")
	}

	if cc.sampleRate == "" {
		return errors.New(`Sample rate not provided. Use the --rate flag to provide a sample rate in the format of "[0-9]+/[mhd]". For example, "12/m".`)
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
		return errors.Wrap(err, "Failed to create an aggregates list.")
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

	err = tsdb.CreateTSDB(cc.rootCommandeer.v3iocfg, &schema)
	if err == nil {
		fmt.Printf("Successfully created TSDB table '%s' in container '%s' of web-API endpoint '%s'.\n", cc.rootCommandeer.v3iocfg.TablePath, cc.rootCommandeer.v3iocfg.Container, cc.rootCommandeer.v3iocfg.WebApiEndpoint)
	}
	return err
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
	parsingError := errors.New(`Invalid rate. The sample rate must be of the format "[0-9]+/[mhd]". For example, "12/m".`)

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
