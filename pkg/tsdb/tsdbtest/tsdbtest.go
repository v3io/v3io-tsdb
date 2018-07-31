package tsdbtest

import (
	"github.com/v3io/v3io-tsdb/pkg/config"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"testing"
)

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
	dbConfig := config.DBPartConfig{
		DaysPerObj:     1,
		HrInChunk:      1,
		DefaultRollups: "sum",
		RollupMin:      10,
	}

	if err := CreateTSDB(v3ioConfig, &dbConfig); err != nil {
		t.Fatalf("Failed to create TSDB. reason: %s", err)
	}

	return func() {
		DeleteTSDB(t, v3ioConfig)
	}
}

func SetUpWithDBConfig(t *testing.T, v3ioConfig *config.V3ioConfig, dbConfig config.DBPartConfig) func() {
	if err := CreateTSDB(v3ioConfig, &dbConfig); err != nil {
		t.Fatalf("Failed to create TSDB. reason: %s", err)
	}

	return func() {
		DeleteTSDB(t, v3ioConfig)
	}
}
