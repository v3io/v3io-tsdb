package tsdbtest

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/pkg/config"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
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

var testDbConfig = config.DBPartConfig{
	DaysPerObj:     1,
	HrInChunk:      1,
	DefaultRollups: "sum",
	RollupMin:      10,
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
	if err := CreateTSDB(v3ioConfig, &testDbConfig); err != nil {
		t.Fatalf("Failed to create TSDB. reason: %s", err)
	}

	return func() {
		DeleteTSDB(t, v3ioConfig)
	}
}

func SetUpWithData(t *testing.T, v3ioConfig *config.V3ioConfig, metricName string, data []DataPoint, userLabels utils.Labels) (*V3ioAdapter, func()) {
	teardown := SetUp(t, v3ioConfig)
	adapter := InsertData(t, v3ioConfig, metricName, data, userLabels)
	return adapter, teardown
}

func SetUpWithDBConfig(t *testing.T, v3ioConfig *config.V3ioConfig, dbConfig config.DBPartConfig) func() {
	if err := CreateTSDB(v3ioConfig, &dbConfig); err != nil {
		t.Fatalf("Failed to create TSDB. reason: %s", err)
	}

	return func() {
		DeleteTSDB(t, v3ioConfig)
	}
}

func InsertData(t *testing.T, v3ioConfig *config.V3ioConfig, metricName string, data []DataPoint, userLabels utils.Labels) *V3ioAdapter {
	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatalf("Failed to get appender. reason: %s", err)
	}

	labels := utils.Labels{utils.Label{Name: "__name__", Value: metricName}}
	labels = append(labels, userLabels...)

	ref, err := appender.Add(labels, data[0].Time, data[0].Value)
	if err != nil {
		t.Fatalf("Failed to add data to appender. reason: %s", err)
	}
	for _, curr := range data[1:] {
		appender.AddFast(labels, ref, curr.Time, curr.Value)
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		t.Fatalf("Failed to wait for appender completion. reason: %s", err)
	}

	return adapter
}
