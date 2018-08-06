package tsdbtest

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/pkg/config"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"regexp"
	"strings"
	"testing"
	"time"
)

type DataPoint struct {
	Time  int64
	Value float64
}

// DataPointTimeSorter sorts DataPoints by time
type DataPointTimeSorter []DataPoint

func (a DataPointTimeSorter) Len() int           { return len(a) }
func (a DataPointTimeSorter) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DataPointTimeSorter) Less(i, j int) bool { return a[i].Time < a[j].Time }

type Sample struct {
	Lset  utils.Labels
	Time  string
	Value float64
}

func DeleteTSDB(t testing.TB, v3ioConfig *config.V3ioConfig) {
	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create adapter. reason: %s", err)
	}

	if err := adapter.DeleteDB(true, true); err != nil {
		t.Fatalf("Failed to delete DB on teardown. reason: %s", err)
	}
}

func CreateTestTSDB(t testing.TB, v3ioConfig *config.V3ioConfig) {
	dbConfig := config.DBPartConfig{
		DaysPerObj:     1,
		HrInChunk:      1,
		DefaultRollups: "*",
		RollupMin:      10,
	}
	if err := CreateTSDB(v3ioConfig, &dbConfig); err != nil {
		t.Fatalf("Failed to create TSDB. reason: %s", err)
	}
}

func SetUp(t testing.TB, v3ioConfig *config.V3ioConfig) func() {
	v3ioConfig.Path = fmt.Sprintf("%s-%d", v3ioConfig.Path, time.Now().Nanosecond())
	CreateTestTSDB(t, v3ioConfig)

	return func() {
		DeleteTSDB(t, v3ioConfig)
	}
}

func SetUpWithDBConfig(t testing.TB, v3ioConfig *config.V3ioConfig, dbConfig config.DBPartConfig) func() {
	v3ioConfig.Path = fmt.Sprintf("%s-%d", v3ioConfig.Path, time.Now().Nanosecond())
	if err := CreateTSDB(v3ioConfig, &dbConfig); err != nil {
		t.Fatalf("Failed to create TSDB. reason: %s", err)
	}

	return func() {
		DeleteTSDB(t, v3ioConfig)
	}
}

func ValidateCountOfSamples(t testing.TB, adapter *V3ioAdapter, metricName string, expected int, startTimeMs, endTimeMs int64) {
	qry, err := adapter.Querier(nil, startTimeMs, endTimeMs)
	if err != nil {
		t.Fatal(err, "failed to create Querier instance.")
	}
	stepSize, err := utils.Str2duration("1h")
	if err != nil {
		t.Fatal(err, "failed to create step")
	}

	set, err := qry.Select("", "count", stepSize, fmt.Sprintf("starts(__name__, '%v')", metricName))

	var actual int
	for set.Next() {
		if set.Err() != nil {
			t.Fatal(set.Err(), "failed to get next element from result set")
		}

		series := set.At()
		iter := series.Iterator()
		for iter.Next() {
			if iter.Err() != nil {
				t.Fatal(set.Err(), "failed to get next time-value pair from iterator")
			}

			_, v := iter.At()
			actual += int(v)
		}
	}

	if expected != actual {
		t.Fatalf("Check failed: actual result is not as expected (%d != %d)", expected, actual)
	}
}

func NormalizePath(path string) string {
	chars := []string{":", "+"}
	r := strings.Join(chars, "")
	re := regexp.MustCompile("[" + r + "]+")
	return re.ReplaceAllString(path, "_")
}
