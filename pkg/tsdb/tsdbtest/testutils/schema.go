package testutils

import (
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"testing"
)

func CreateSchema(t testing.TB, agg string) config.Schema {
	rollups, err := aggregate.AggregatorsToStringList(agg)
	if err != nil {
		t.Fatal(err)
	}
	defaultRollup := config.Rollup{
		Aggregators:            rollups,
		AggregatorsGranularity: "1h",
		StorageClass:           "local",
		SampleRetention:        0,
		LayerRetentionTime:     "1y",
	}

	tableSchema := config.TableSchema{
		Version:             0,
		RollupLayers:        []config.Rollup{defaultRollup},
		ShardingBuckets:     8,
		PartitionerInterval: "340h",
		ChunckerInterval:    "10h",
	}

	fields, err := aggregate.SchemaFieldFromString(rollups, "v")
	if err != nil {
		t.Fatal("Failed to create aggregators list", err)
	}
	fields = append(fields, config.SchemaField{Name: "_name", Type: "string", Nullable: false, Items: ""})

	partitionSchema := config.PartitionSchema{
		Version:                tableSchema.Version,
		Aggregators:            rollups,
		AggregatorsGranularity: "1h",
		StorageClass:           "local",
		SampleRetention:        0,
		ChunckerInterval:       tableSchema.ChunckerInterval,
		PartitionerInterval:    tableSchema.PartitionerInterval,
	}

	schema := config.Schema{
		TableSchemaInfo:     tableSchema,
		PartitionSchemaInfo: partitionSchema,
		Partitions:          []config.Partition{},
		Fields:              fields,
	}
	return schema
}
