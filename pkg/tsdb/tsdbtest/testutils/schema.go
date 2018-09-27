package testutils

import (
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	"testing"
)

func CreateSchema(t testing.TB, aggregators string) *config.Schema {
	aggs, err := aggregate.AggregatorsToStringList(aggregators)
	if err != nil {
		t.Fatal(err)
	}

	schm, err := schema.NewSchema(aggs)

	if err != nil {
		t.Fatal(err)
	}
	return schm
}
