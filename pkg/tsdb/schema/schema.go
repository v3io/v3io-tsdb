package schema

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strconv"
	"strings"
	"time"
)

const (
	Version = 0
)

func NewSchema(v3ioCfg *config.V3ioConfig, sampleRate, aggregatorGranularity, aggregatesList string) (*config.Schema, error) {
	return newSchema(
		sampleRate,
		aggregatorGranularity,
		aggregatesList,
		v3ioCfg.MinimumChunkSize,
		v3ioCfg.MaximumChunkSize,
		v3ioCfg.MaximumSampleSize,
		v3ioCfg.MaximumPartitionSize,
		config.DefaultSampleRetentionTime,
		v3ioCfg.ShardingBuckets)
}

func newSchema(sampleRate, aggregatorGranularity, aggregatesList string, minChunkSize, maxChunkSize, maxSampleSize, maxPartitionSize, sampleRetention, shardingBuckets int) (*config.Schema, error) {
	rateInHours, err := rateToHours(sampleRate)
	if err != nil {
		return nil, errors.Wrap(err, "invalid sample race")
	}

	if err := validateAggregatorGranularity(aggregatorGranularity); err != nil {
		return nil, errors.Wrap(err, "failed to parse aggregator granularity")
	}

	chunkInterval, partitionInterval, err := calculatePartitionAndChunkInterval(rateInHours, minChunkSize, maxChunkSize, maxSampleSize, maxPartitionSize)
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate chunk interval")
	}

	aggregates, err := aggregate.AggregatorsToStringList(aggregatesList)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse list of aggregates")
	}

	defaultRollup := config.Rollup{
		Aggregators:            []string{},
		AggregatorsGranularity: aggregatorGranularity,
		StorageClass:           config.DefaultStorageClass,
		SampleRetention:        sampleRetention,                  //TODO: make configurable
		LayerRetentionTime:     config.DefaultLayerRetentionTime, //TODO: make configurable
	}

	tableSchema := config.TableSchema{
		Version:             Version,
		RollupLayers:        []config.Rollup{defaultRollup},
		ShardingBuckets:     shardingBuckets,
		PartitionerInterval: partitionInterval,
		ChunckerInterval:    chunkInterval,
	}

	if len(aggregates) == 0 {
		aggregates = strings.Split(config.DefaultAggregates, ",")
	}

	fields, err := aggregate.SchemaFieldFromString(aggregates, "v")
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create list of aggregates from: '%s'", aggregates)
	}
	fields = append(fields, config.SchemaField{Name: "_name", Type: "string", Nullable: false, Items: ""})

	partitionSchema := config.PartitionSchema{
		Version:                tableSchema.Version,
		Aggregators:            aggregates,
		AggregatorsGranularity: config.DefaultAggregationGranularity,
		StorageClass:           config.DefaultStorageClass,
		SampleRetention:        config.DefaultSampleRetentionTime,
		ChunckerInterval:       tableSchema.ChunckerInterval,
		PartitionerInterval:    tableSchema.PartitionerInterval,
	}

	schema := &config.Schema{
		TableSchemaInfo:     tableSchema,
		PartitionSchemaInfo: partitionSchema,
		Partitions:          []*config.Partition{},
		Fields:              fields,
	}

	return schema, nil
}

func calculatePartitionAndChunkInterval(rateInHours, minChunkSize, maxChunkSize, maxSampleSize, maxPartitionSize int) (string, string, error) {
	maxNumberOfEventsPerChunk := maxChunkSize / maxSampleSize
	minNumberOfEventsPerChunk := minChunkSize / maxSampleSize

	chunkInterval := maxNumberOfEventsPerChunk / rateInHours
	if chunkInterval == 0 {
		return "", "", errors.New("sample rate is too high")
	}

	// Make sure the expected chunk size is greater then the supported minimum.
	if chunkInterval < minNumberOfEventsPerChunk/rateInHours {
		return "", "", fmt.Errorf(
			"calculated chunk size is less than minimum, rate - %v/h, calculated chunk interval - %v, minimum size - %v",
			rateInHours, chunkInterval, minChunkSize)
	}

	actualCapacityOfChunk := chunkInterval * rateInHours * maxSampleSize
	numberOfChunksInPartition := 0

	for (numberOfChunksInPartition+24)*actualCapacityOfChunk < maxPartitionSize {
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

func validateAggregatorGranularity(aggregatorGranularity string) error {
	dayMillis := 24 * int64(time.Hour/time.Millisecond)
	duration, err := utils.Str2duration(aggregatorGranularity)
	if err != nil {
		return err
	}

	if dayMillis%duration != 0 && duration%dayMillis != 0 {
		return errors.New("rollup interval should be a divisor or a dividend of 1 day. Example: 10m, 30m, 2h, etc.")
	}
	return nil
}
