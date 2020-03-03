package schema

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const (
	Version          = 4
	MaxV3ioArraySize = 130000
)

func NewSchema(v3ioCfg *config.V3ioConfig, samplesIngestionRate, aggregationGranularity, aggregatesList string, crossLabelSets string) (*config.Schema, error) {
	return newSchema(
		samplesIngestionRate,
		aggregationGranularity,
		aggregatesList,
		crossLabelSets,
		v3ioCfg.MinimumChunkSize,
		v3ioCfg.MaximumChunkSize,
		v3ioCfg.MaximumSampleSize,
		v3ioCfg.MaximumPartitionSize,
		config.DefaultSampleRetentionTime,
		v3ioCfg.ShardingBucketsCount)
}

func newSchema(samplesIngestionRate, aggregationGranularity, aggregatesList string, crossLabelSets string, minChunkSize, maxChunkSize, maxSampleSize, maxPartitionSize, sampleRetention, shardingBucketsCount int) (*config.Schema, error) {
	rateInHours, err := rateToHours(samplesIngestionRate)
	if err != nil {
		return nil, errors.Wrapf(err, "Invalid samples ingestion rate (%s).", samplesIngestionRate)
	}

	chunkInterval, partitionInterval, err := calculatePartitionAndChunkInterval(rateInHours, minChunkSize, maxChunkSize, maxSampleSize, maxPartitionSize)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to calculate the chunk interval.")
	}

	aggregates, err := aggregate.RawAggregatesToStringList(aggregatesList)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to parse aggregates list '%s'.", aggregatesList)
	}

	if err := validateAggregatesGranularity(aggregationGranularity, partitionInterval, len(aggregates) > 0); err != nil {
		return nil, err
	}

	parsedCrossLabelSets := aggregate.ParseCrossLabelSets(crossLabelSets)

	if len(parsedCrossLabelSets) > 0 && len(aggregates) == 0 {
		return nil, errors.New("Cross label aggregations must be used in conjunction with aggregations")
	}

	if len(aggregates) == 0 {
		aggregates = strings.Split(config.DefaultAggregates, ",")
	}

	defaultRollup := config.Rollup{
		Aggregates:             aggregates,
		AggregationGranularity: aggregationGranularity,
		StorageClass:           config.DefaultStorageClass,
		SampleRetention:        sampleRetention, //TODO: make configurable
		LayerRetentionTime:     config.DefaultLayerRetentionTime,
	}

	var preaggregates []config.PreAggregate
	for _, labelSet := range parsedCrossLabelSets {
		preaggregate := config.PreAggregate{
			Labels:      labelSet,
			Granularity: aggregationGranularity,
			Aggregates:  aggregates,
		}
		preaggregates = append(preaggregates, preaggregate)
	}

	tableSchema := config.TableSchema{
		Version:              Version,
		RollupLayers:         []config.Rollup{defaultRollup},
		ShardingBucketsCount: shardingBucketsCount,
		PartitionerInterval:  partitionInterval,
		ChunckerInterval:     chunkInterval,
		PreAggregates:        preaggregates,
	}

	fields, err := aggregate.SchemaFieldFromString(aggregates, "v")
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create an aggregates list from string '%s'.", aggregates)
	}
	fields = append(fields, config.SchemaField{Name: "_name", Type: "string", Nullable: false, Items: ""})

	partitionSchema := config.PartitionSchema{
		Version:                tableSchema.Version,
		Aggregates:             aggregates,
		AggregationGranularity: aggregationGranularity,
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
		return "", "", fmt.Errorf("the samples ingestion rate (%v/h) is too high", rateInHours)
	}

	// Make sure the expected chunk size is greater then the supported minimum.
	if chunkInterval < minNumberOfEventsPerChunk/rateInHours {
		return "", "", fmt.Errorf(
			"the calculated chunk size is smaller than the minimum: samples ingestion rate = %v/h, calculated chunk interval = %v, minimum size = %v",
			rateInHours, chunkInterval, minChunkSize)
	}

	actualCapacityOfChunk := chunkInterval * rateInHours * maxSampleSize
	numberOfChunksInPartition := 0

	for (numberOfChunksInPartition+24)*actualCapacityOfChunk < maxPartitionSize {
		numberOfChunksInPartition += 24
	}
	if numberOfChunksInPartition == 0 {
		return "", "", errors.Errorf("the samples ingestion rate (%v/h) is too high - cannot fit a partition in a day interval with the calculated chunk size (%v)", rateInHours, chunkInterval)
	}

	partitionInterval := numberOfChunksInPartition * chunkInterval
	return strconv.Itoa(chunkInterval) + "h", strconv.Itoa(partitionInterval) + "h", nil
}

func rateToHours(samplesIngestionRate string) (int, error) {
	parsingError := errors.New(`Invalid samples ingestion rate. The rate must be of the format "[0-9]+/[smh]". For example, "12/m"`)

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
		return 0, fmt.Errorf("invalid samples ingestion rate (%s). The rate cannot have a negative number of samples", samplesIngestionRate)
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

func validateAggregatesGranularity(aggregationGranularity string, partitionInterval string, hasAggregates bool) error {
	dayMillis := 24 * int64(time.Hour/time.Millisecond)
	duration, err := utils.Str2duration(aggregationGranularity)
	if err != nil {
		return errors.Wrapf(err, "Failed to parse aggregation granularity '%s'.", aggregationGranularity)
	}

	if dayMillis%duration != 0 && duration%dayMillis != 0 {
		return errors.New("the aggregation granularity should be a divisor or a dividend of 1 day. Examples: \"10m\"; \"30m\"; \"2h\"")
	}

	if hasAggregates {
		partitionIntervalDuration, _ := utils.Str2duration(partitionInterval) // safe to ignore error since we create 'partitionInterval'
		if partitionIntervalDuration/duration > MaxV3ioArraySize {
			return errors.New("the size of the aggregation-granularity interval isn't sufficiently larger than the specified ingestion rate. Try increasing the granularity to get the expected pre-aggregation performance impact")
		}
	}
	return nil
}
