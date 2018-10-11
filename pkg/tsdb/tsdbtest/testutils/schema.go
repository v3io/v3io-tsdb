package testutils

import (
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	"testing"
)

func CreateSchema(t testing.TB, aggregates string) *config.Schema {
	v3ioCfg, err := config.GetOrDefaultConfig()
	if err != nil {
		t.Fatalf("Failed to obtain a TSDB configuration. Error: %v", err)
	}

	schm, err := schema.NewSchema(v3ioCfg, "1/s", "1h", aggregates)
	if err != nil {
		t.Fatalf("Failed to create a TSDB schema. Error: %v", err)
	}
	return schm
}
