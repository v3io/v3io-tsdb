package tsdbtest

import (
	json2 "encoding/json"
	"fmt"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/config"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest/testutils"
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

	now := time.Now().Unix() * 1000 // now time in millis
	if err := adapter.DeleteDB(true, true, 0, now); err != nil {
		t.Fatalf("Failed to delete DB on teardown. reason: %s", err)
	}
}

func CreateTestTSDB(t testing.TB, v3ioConfig *config.V3ioConfig) {
	schema := testutils.CreateSchema(t, "*")
	if err := CreateTSDB(v3ioConfig, schema); err != nil {
		v3ioConfigAsJson, _ := json2.MarshalIndent(v3ioConfig, "", "  ")
		t.Fatalf("Failed to create TSDB. Reason: %v\nConfiguration:\n%s", err, string(v3ioConfigAsJson))
	}
}

func SetUp(t testing.TB, v3ioConfig *config.V3ioConfig) func() {
	v3ioConfig.TablePath = fmt.Sprintf("%s-%d", t.Name(), time.Now().Nanosecond())
	CreateTestTSDB(t, v3ioConfig)

	// Measure performance
	metricReporter, err := performance.DefaultReporterInstance()
	if err != nil {
		t.Fatalf("unable to initialize performance metrics reporter. Reason: %v", err)
	}
	metricReporter.Start()

	return func() {
		defer metricReporter.Stop()
		// Don't delete the table if the test has failed
		if !t.Failed() {
			DeleteTSDB(t, v3ioConfig)
		}
	}
}

func SetUpWithData(t *testing.T, v3ioConfig *config.V3ioConfig, metricName string, data []DataPoint, userLabels utils.Labels) (*V3ioAdapter, func()) {
	teardown := SetUp(t, v3ioConfig)
	adapter := InsertData(t, v3ioConfig, metricName, data, userLabels)
	return adapter, teardown
}

func SetUpWithDBConfig(t *testing.T, v3ioConfig *config.V3ioConfig, schema *config.Schema) func() {
	v3ioConfig.TablePath = fmt.Sprintf("%s-%d", t.Name(), time.Now().Nanosecond())
	if err := CreateTSDB(v3ioConfig, schema); err != nil {
		v3ioConfigAsJson, _ := json2.MarshalIndent(v3ioConfig, "", "  ")
		t.Fatalf("Failed to create TSDB. Reason: %s\nConfiguration:\n%s", err, string(v3ioConfigAsJson))
	}

	// Measure performance
	metricReporter, err := performance.DefaultReporterInstance()
	if err != nil {
		t.Fatalf("unable to initialize performance metrics reporter. Error: %v", err)
	}
	metricReporter.Start()

	return func() {
		defer metricReporter.Stop()
		// Don't delete the table if the test has failed
		if !t.Failed() {
			DeleteTSDB(t, v3ioConfig)
		}
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

func ValidateCountOfSamples(t testing.TB, adapter *V3ioAdapter, metricName string, expected int, startTimeMs, endTimeMs int64, queryAggStep int64) {

	var stepSize int64
	if queryAggStep <= 0 {
		var err error
		stepSize, err = utils.Str2duration("1h")
		if err != nil {
			t.Fatal(err, "failed to create step")
		}
	} else {
		stepSize = queryAggStep
	}

	qry, err := adapter.Querier(nil, startTimeMs-stepSize, endTimeMs)
	if err != nil {
		t.Fatal(err, "failed to create Querier instance.")
	}

	set, err := qry.Select("", "count", stepSize, fmt.Sprintf("starts(__name__, '%v')", metricName))

	var actualCount int
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
			actualCount += int(v)
		}
	}

	if set.Err() != nil {
		t.Fatal(set.Err())
	}

	if expected != actualCount {
		t.Fatalf("Check failed: actualCount result is not as expected [%d(actualCount) != %d(expected)]", actualCount, expected)
	}

	t.Logf("PASS: total count of samples, i.e. [%d(actualCount) == %d(expected)]", actualCount, expected)
}

func ValidateRawData(t testing.TB, adapter *V3ioAdapter, metricName string, startTimeMs, endTimeMs int64, isValid func(*DataPoint, *DataPoint) bool) {

	qry, err := adapter.Querier(nil, startTimeMs, endTimeMs)
	if err != nil {
		t.Fatal(err, "failed to create Querier instance.")
	}

	set, err := qry.Select(metricName, "", 0, "")

	for set.Next() {
		// start over for each label set
		var lastDataPoint = &DataPoint{Time: -1, Value: -1.0}

		if set.Err() != nil {
			t.Fatal(set.Err(), "failed to get next element from result set")
		}

		series := set.At()
		iter := series.Iterator()
		for iter.Next() {
			if iter.Err() != nil {
				t.Fatal(set.Err(), "failed to get next time-value pair from iterator")
			}
			currentTime, currentValue := iter.At()
			currentDataPoint := &DataPoint{Time: currentTime, Value: currentValue}

			if lastDataPoint.Value >= 0 {
				// Note, we cast float to integer to eliminate the risk of precision error
				if !isValid(lastDataPoint, currentDataPoint) {
					t.Fatalf("Check for consistency of the raw data has failed: metric name: '%s'\n\tisValid(%v, %v) == false",
						metricName, lastDataPoint, currentDataPoint)
				}
			}
			lastDataPoint = currentDataPoint
		}
	}

	if set.Err() != nil {
		t.Fatal(set.Err())
	}
}

func NormalizePath(path string) string {
	chars := []string{":", "+"}
	r := strings.Join(chars, "")
	re := regexp.MustCompile("[" + r + "]+")
	return re.ReplaceAllString(path, "_")
}
