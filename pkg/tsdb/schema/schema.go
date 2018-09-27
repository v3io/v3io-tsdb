package schema

import (
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"strings"
)

const (
	SchemaVersion                 = 0
	DefaultStorageClass           = "local"
	DefaultIngestionRate          = ""
	DefaultAggregates             = "" // no aggregates by default
	DefaultAggregationGranularity = "1h"
	DefaultShardingBuckets        = 8
	DefaultLayerRetentionTime     = "1y"
	DefaultSampleRetentionTime    = 0
	DefaultPartitionInterval      = "48h"
	DefaultChunkInterval          = "1h"
)

func NewSchema(aggregates []string) (*config.Schema, error) {
	defaultRollup := config.Rollup{
		Aggregators:            []string{},
		AggregatorsGranularity: "1h",
		StorageClass:           DefaultStorageClass,
		SampleRetention:        DefaultSampleRetentionTime, //TODO: make configurable
		LayerRetentionTime:     DefaultLayerRetentionTime,  //TODO: make configurable
	}

	tableSchema := config.TableSchema{
		Version:             SchemaVersion,
		RollupLayers:        []config.Rollup{defaultRollup},
		ShardingBuckets:     DefaultShardingBuckets,
		PartitionerInterval: DefaultPartitionInterval,
		ChunckerInterval:    DefaultChunkInterval,
	}

	if len(aggregates) == 0 {
		aggregates = strings.Split(DefaultAggregates, ",")
	}

	fields, err := aggregate.SchemaFieldFromString(aggregates, "v")
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create list of aggregates from: '%s'", aggregates)
	}
	fields = append(fields, config.SchemaField{Name: "_name", Type: "string", Nullable: false, Items: ""})

	partitionSchema := config.PartitionSchema{
		Version:                tableSchema.Version,
		Aggregators:            aggregates,
		AggregatorsGranularity: DefaultAggregationGranularity,
		StorageClass:           DefaultStorageClass,
		SampleRetention:        DefaultSampleRetentionTime,
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
