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
	Version = 1
)

func NewSchema(v3ioCfg *config.V3ioConfig, samplesIngestionRate, aggregationGranularity, aggregatesList string) (*config.Schema, error) {
	return newSchema(
		samplesIngestionRate,
		aggregationGranularity,
		aggregatesList,
		v3ioCfg.MinimumChunkSize,
		v3ioCfg.MaximumChunkSize,
		v3ioCfg.MaximumSampleSize,
		v3ioCfg.MaximumPartitionSize,
		config.DefaultSampleRetentionTime,
		v3ioCfg.ShardingBucketsCount)
}

func newSchema(samplesIngestionRate, aggregationGranularity, aggregatesList string, minChunkSize, maxChunkSize, maxSampleSize, maxPartitionSize, sampleRetention, shardingBucketsCount int) (*config.Schema, error) {
	rateInHours, err := rateToHours(samplesIngestionRate)
	if err != nil {
		return nil, errors.Wrapf(err, "Invalid samples ingestion rate (%s).", samplesIngestionRate)
	}

	if err := validateAggregatesGranularity(aggregationGranularity); err != nil {
		return nil, errors.Wrapf(err, "Failed to parse aggregation granularity '%s'.", aggregationGranularity)
	}

	chunkInterval, partitionInterval, err := calculatePartitionAndChunkInterval(rateInHours, minChunkSize, maxChunkSize, maxSampleSize, maxPartitionSize)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to calculate the chunk interval.")
	}

	aggregates, err := aggregate.AggregatesToStringList(aggregatesList)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to parse aggregates list '%s'.", aggregatesList)
	}

	defaultRollup := config.Rollup{
		Aggregates:             []string{},
		AggregationGranularity: aggregationGranularity,
		StorageClass:           config.DefaultStorageClass,
		SampleRetention:        sampleRetention,                  //TODO: make configurable
		LayerRetentionTime:     config.DefaultLayerRetentionTime, //TODO: make configurable
	}

	tableSchema := config.TableSchema{
		Version:              Version,
		RollupLayers:         []config.Rollup{defaultRollup},
		ShardingBucketsCount: shardingBucketsCount,
		PartitionerInterval:  partitionInterval,
		ChunckerInterval:     chunkInterval,
	}

	if len(aggregates) == 0 {
		aggregates = strings.Split(config.DefaultAggregates, ",")
	}

	fields, err := aggregate.SchemaFieldFromString(aggregates, "v")
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create an aggregates list from string '%s'.", aggregates)
	}
	fields = append(fields, config.SchemaField{Name: "_name", Type: "string", Nullable: false, Items: ""})

	partitionSchema := config.PartitionSchema{
		Version:                tableSchema.Version,
		Aggregates:             aggregates,
		AggregationGranularity: config.DefaultAggregationGranularity,
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
		return "", "", fmt.Errorf("The samples ingestion rate (%v/h) is too high.", rateInHours)
	}

	// Make sure the expected chunk size is greater then the supported minimum.
	if chunkInterval < minNumberOfEventsPerChunk/rateInHours {
		return "", "", fmt.Errorf(
			"The calculated chunk size is smaller than the minimum: samples ingestion rate = %v/h, calculated chunk interval = %v, minimum size = %v",
			rateInHours, chunkInterval, minChunkSize)
	}

	actualCapacityOfChunk := chunkInterval * rateInHours * maxSampleSize
	numberOfChunksInPartition := 0

	for (numberOfChunksInPartition+24)*actualCapacityOfChunk < maxPartitionSize {
		numberOfChunksInPartition += 24
	}
	if numberOfChunksInPartition == 0 {
		return "", "", errors.Errorf("The samples ingestion rate (%v/h) is too high - cannot fit a partition in a day interval with the calculated chunk size (%v).", rateInHours, chunkInterval)
	}

	partitionInterval := numberOfChunksInPartition * chunkInterval
	return strconv.Itoa(chunkInterval) + "h", strconv.Itoa(partitionInterval) + "h", nil
}

func rateToHours(samplesIngestionRate string) (int, error) {
	parsingError := errors.New(`Invalid samples ingestion rate. The rate must be of the format "[0-9]+/[mhd]". For example, "12/m".`)

	if len(samplesIngestionRate) < 3 {
		return 0, parsingError
	}
	if samplesIngestionRate[len(samplesIngestionRate)-2] != '/' {
		return 0, parsingError
	}

	last := samplesIngestionRate[len(samplesIngestionRate)-1]
	// Get the ingestion-rate samples number, ignoring the slash and time unit
	samplesIngestionRate = samplesIngestionRate[:len(samplesIngestionRate)-2]
	i, err := strconv.Atoi(samplesIngestionRate)
	if err != nil {
		return 0, errors.Wrap(err, parsingError.Error())
	}
	if i <= 0 {
		return 0, fmt.Errorf("Invalid samples ingestion rate (%s). The rate cannot have a negative number of samples.", samplesIngestionRate)
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

func validateAggregatesGranularity(aggregationGranularity string) error {
	dayMillis := 24 * int64(time.Hour/time.Millisecond)
	duration, err := utils.Str2duration(aggregationGranularity)
	if err != nil {
		return err
	}

	if dayMillis%duration != 0 && duration%dayMillis != 0 {
		return errors.New("The aggregation granularity should be a divisor or a dividend of 1 day. Examples: \"10m\"; \"30m\"; \"2h\".")
	}
	return nil
}
