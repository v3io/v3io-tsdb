package common

import (
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"strings"
)

func CreateTSDB(v3ioConfig *config.V3ioConfig, newTsdbPath string) error {
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
		return errors.Wrap(err, "Failed to create aggregators list")
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

	schema := config.Schema{
		TableSchemaInfo:     tableSchema,
		PartitionSchemaInfo: partitionSchema,
		Partitions:          []config.Partition{},
		Fields:              fields,
	}
	return tsdb.CreateTSDB(v3ioConfig, &schema)
}

func DeleteTSDB(adapter *tsdb.V3ioAdapter, deleteConf bool, force bool) {
	adapter.DeleteDB(deleteConf, force)
}
