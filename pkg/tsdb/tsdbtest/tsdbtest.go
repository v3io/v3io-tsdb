package tsdbtest

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
	"testing"
	"time"
)

type DataPoint struct {
	Time  int64
	Value float64
}

type Sample struct {
	Lset  utils.Labels
	Time  string
	Value float64
}

func DeleteTSDB(t *testing.T, v3ioConfig *config.V3ioConfig) {
	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create adapter. reason: %s", err)
	}

	if err := adapter.DeleteDB(true, true); err != nil {
		t.Fatalf("Failed to delete DB on teardown. reason: %s", err)
	}
}

func SetUp(t *testing.T, v3ioConfig *config.V3ioConfig) func() {
	v3ioConfig.Path = fmt.Sprintf("%s-%d", v3ioConfig.Path, time.Now().Nanosecond())
	schema, err := createSchema()
	if err != nil {
		t.Fatalf("Failed to create schema. Error: %s", err)
	}

	if err := CreateTSDB(v3ioConfig, schema); err != nil {
		t.Fatalf("Failed to create TSDB. Error: %s", err)
	}

	return func() {
		DeleteTSDB(t, v3ioConfig)
	}
}

func SetUpWithDBConfig(t *testing.T, v3ioConfig *config.V3ioConfig, schema *config.Schema) func() {
	if err := CreateTSDB(v3ioConfig, schema); err != nil {
		t.Fatalf("Failed to create TSDB. reason: %s", err)
	}

	return func() {
		DeleteTSDB(t, v3ioConfig)
	}
}

// TODO: refactor - move to commmot test infra
func createSchema() (schema *config.Schema, err error) {
	defaultRollup := config.Rollup{
		Aggregators:                     "*",
		AggregatorsGranularityInSeconds: 3600,
		StorageClass:                    "local",
		SampleRetention:                 0,
		LayerRetentionTime:              "1Y",
	}

	tableSchema := config.TableSchema{
		Version:             0,
		RollupLayers:        []config.Rollup{defaultRollup},
		ShardingBuckets:     64,
		PartitionerInterval: "1D",
		ChunckerInterval:    "1H",
	}

	aggrs := strings.Split("*", ",")
	fields, err := aggregate.SchemaFieldFromString(aggrs, "v")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create list of aggregator functions")
	}
	fields = append(fields, config.SchemaField{Name: "_name", Type: "string", Nullable: false, Items: ""})

	partitionSchema := config.PartitionSchema{
		Version:                         tableSchema.Version,
		Aggregators:                     aggrs,
		AggregatorsGranularityInSeconds: 3600,
		StorageClass:                    "local",
		SampleRetention:                 0,
		ChunckerInterval:                tableSchema.ChunckerInterval,
		PartitionerInterval:             tableSchema.PartitionerInterval,
	}

	schema = &config.Schema{
		TableSchemaInfo:     tableSchema,
		PartitionSchemaInfo: partitionSchema,
		Partitions:          []config.Partition{},
		Fields:              fields,
	}
	return schema, err
}
