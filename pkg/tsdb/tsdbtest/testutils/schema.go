package testutils

import (
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	"testing"
)

func CreateSchema(t testing.TB, aggregators string) *config.Schema {
	schm, err := schema.NewDefaultSchema(aggregators)

	if err != nil {
		t.Fatal(err)
	}
	return schm
}
