package testutils

import (
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	"testing"
)

func CreateSchema(t testing.TB, aggregators string) *config.Schema {
	v3ioCfg, err := config.GetOrDefaultConfig()
	if err != nil {
		t.Fatalf("failed to obtain V3IO configuration. Error: %v", err)
	}

	schm, err := schema.NewSchema(v3ioCfg, "1/s", "1h", aggregators)
	if err != nil {
		t.Fatalf("failed to create schema. Error: %v", err)
	}
	return schm
}
