package tsdbtest

import (
	json2 "encoding/json"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest/testutils"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const MinuteInMillis = 60 * 1000
const HoursInMillis = 60 * MinuteInMillis
const DaysInMillis = 24 * HoursInMillis

type DataPoint struct {
	Time  int64
	Value float64
}
type Metric struct {
	Name   string
	Labels utils.Labels
	Data   []DataPoint
}
type TimeSeries []Metric

const OptDropTableOnTearDown = "DropTableOnTearDown"
const OptIgnoreReason = "IgnoreReason"
const OptTimeSeries = "TimeSeries"
const OptV3ioConfig = "V3ioConfig"

type TestParams map[string]interface{}
type TestOption struct {
	Key   string
	Value interface{}
}

func NewTestParams(t testing.TB, opts ...TestOption) TestParams {
	initialSize := len(opts)
	testOpts := make(TestParams, initialSize)

	// Initialize defaults
	testOpts[OptDropTableOnTearDown] = true
	testOpts[OptIgnoreReason] = ""
	testOpts[OptTimeSeries] = TimeSeries{}

	defaultV3ioConfig, err := LoadV3ioConfig()
	if err != nil {
		t.Fatalf("Unable to get V3IO configuration.\nError: %v", err)
	}

	//defaultV3ioConfig.TablePath = PrefixTablePath(t.Name())
	testOpts[OptV3ioConfig] = defaultV3ioConfig

	for _, opt := range opts {
		testOpts[opt.Key] = opt.Value
	}

	return testOpts
}

func (tp TestParams) TimeSeries() TimeSeries {
	return tp[OptTimeSeries].(TimeSeries)
}
func (tp TestParams) DropTableOnTearDown() bool {
	return tp[OptDropTableOnTearDown].(bool)
}
func (tp TestParams) IgnoreReason() string {
	return tp[OptIgnoreReason].(string)
}
func (tp TestParams) V3ioConfig() *config.V3ioConfig {
	return tp[OptV3ioConfig].(*config.V3ioConfig)
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
		t.Fatalf("Failed to create an adapter. Reason: %s", err)
	}

	now := time.Now().Unix() * 1000 // Current time (now) in milliseconds
	if err := adapter.DeleteDB(true, true, 0, now); err != nil {
		t.Fatalf("Failed to delete a TSDB instance (table) on teardown. Reason: %s", err)
	}
}

func CreateTestTSDB(t testing.TB, v3ioConfig *config.V3ioConfig) {
	CreateTestTSDBWithAggregates(t, v3ioConfig, "*")
}

func CreateTestTSDBWithAggregates(t testing.TB, v3ioConfig *config.V3ioConfig, aggregates string) {
	schema := testutils.CreateSchema(t, aggregates)
	if err := CreateTSDB(v3ioConfig, schema); err != nil {
		v3ioConfigAsJson, _ := json2.MarshalIndent(v3ioConfig, "", "  ")
		t.Fatalf("Failed to create a TSDB instance (table). Reason: %v\nConfiguration:\n%s", err, string(v3ioConfigAsJson))
	}
}

func tearDown(t testing.TB, v3ioConfig *config.V3ioConfig, testParams TestParams) {
	// Don't delete the TSDB table if the test failed or test expects that
	if !t.Failed() && testParams.DropTableOnTearDown() {
		DeleteTSDB(t, v3ioConfig)
	}
}

func SetUp(t testing.TB, testParams TestParams) func() {
	v3ioConfig := testParams.V3ioConfig()
	v3ioConfig.TablePath = PrefixTablePath(fmt.Sprintf("%s-%d", t.Name(), time.Now().Nanosecond()))
	CreateTestTSDB(t, v3ioConfig)

	// Measure performance
	metricReporter, err := performance.DefaultReporterInstance()
	if err != nil {
		t.Fatalf("Unable to initialize the performance metrics reporter. Reason: %v", err)
	}
	metricReporter.Start()

	return func() {
		defer metricReporter.Stop()
		tearDown(t, v3ioConfig, testParams)
	}
}

func SetUpWithData(t *testing.T, testOpts TestParams) (*V3ioAdapter, func()) {
	teardown := SetUp(t, testOpts)
	adapter := InsertData(t, testOpts)
	return adapter, teardown
}

func SetUpWithDBConfig(t *testing.T, schema *config.Schema, testParams TestParams) func() {
	v3ioConfig := testParams.V3ioConfig()
	v3ioConfig.TablePath = PrefixTablePath(fmt.Sprintf("%s-%d", t.Name(), time.Now().Nanosecond()))
	if err := CreateTSDB(v3ioConfig, schema); err != nil {
		v3ioConfigAsJson, _ := json2.MarshalIndent(v3ioConfig, "", "  ")
		t.Fatalf("Failed to create a TSDB instance (table). Reason: %s\nConfiguration:\n%s", err, string(v3ioConfigAsJson))
	}

	// Measure performance
	metricReporter, err := performance.DefaultReporterInstance()
	if err != nil {
		t.Fatalf("Unable to initialize the performance metrics reporter. Error: %v", err)
	}
	metricReporter.Start()

	return func() {
		defer metricReporter.Stop()
		tearDown(t, v3ioConfig, testParams)
	}
}

func InsertData(t *testing.T, testParams TestParams) *V3ioAdapter {
	adapter, err := NewV3ioAdapter(testParams.V3ioConfig(), nil, nil)
	if err != nil {
		t.Fatalf("Failed to create a V3IO TSDB adapter. Reason: %s", err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatalf("Failed to get an appender. Reason: %s", err)
	}

	timeSeries := testParams.TimeSeries()

	for _, metric := range timeSeries {

		labels := utils.Labels{utils.Label{Name: "__name__", Value: metric.Name}}
		labels = append(labels, metric.Labels...)

		ref, err := appender.Add(labels, metric.Data[0].Time, metric.Data[0].Value)
		if err != nil {
			t.Fatalf("Failed to add data to the TSDB appender. Reason: %s", err)
		}
		for _, curr := range metric.Data[1:] {
			appender.AddFast(labels, ref, curr.Time, curr.Value)
		}

		if _, err := appender.WaitForCompletion(0); err != nil {
			t.Fatalf("Failed to wait for TSDB append completion. Reason: %s", err)
		}
	}

	return adapter
}

func ValidateCountOfSamples(t testing.TB, adapter *V3ioAdapter, metricName string, expected int, startTimeMs, endTimeMs int64, queryAggStep int64) {

	var stepSize int64
	if queryAggStep <= 0 {
		var err error
		stepSize, err = utils.Str2duration("1h")
		if err != nil {
			t.Fatal(err, "Failed to create an aggregation interval (step).")
		}
	} else {
		stepSize = queryAggStep
	}

	qry, err := adapter.Querier(nil, startTimeMs-stepSize, endTimeMs)
	if err != nil {
		t.Fatal(err, "Failed to create a Querier instance.")
	}

	set, err := qry.Select("", "count", stepSize, fmt.Sprintf("starts(__name__, '%v')", metricName))

	var actualCount int
	for set.Next() {
		if set.Err() != nil {
			t.Fatal(set.Err(), "Failed to get the next element from the result set.")
		}

		series := set.At()
		iter := series.Iterator()
		for iter.Next() {
			if iter.Err() != nil {
				t.Fatal(set.Err(), "Failed to get the next time-value pair from  the iterator.")
			}
			_, v := iter.At()
			actualCount += int(v)
		}
	}

	if set.Err() != nil {
		t.Fatal(set.Err())
	}

	if expected != actualCount {
		t.Fatalf("Check failed: the metric samples' actual count isn't as expected [%d(actualCount) != %d(expected)].", actualCount, expected)
	}

	t.Logf("PASS: the metric-samples actual count matches the expected total count [%d(actualCount) == %d(expected)].", actualCount, expected)
}

func ValidateRawData(t testing.TB, adapter *V3ioAdapter, metricName string, startTimeMs, endTimeMs int64, isValid func(*DataPoint, *DataPoint) bool) {

	qry, err := adapter.Querier(nil, startTimeMs, endTimeMs)
	if err != nil {
		t.Fatal(err, "Failed to create a Querier instance.")
	}

	set, err := qry.Select(metricName, "", 0, "")

	for set.Next() {
		// Start over for each label set
		var lastDataPoint = &DataPoint{Time: -1, Value: -1.0}

		if set.Err() != nil {
			t.Fatal(set.Err(), "Failed to get the next element from a result set.")
		}

		series := set.At()
		iter := series.Iterator()
		for iter.Next() {
			if iter.Err() != nil {
				t.Fatal(set.Err(), "Failed to get the next time-value pair from an iterator.")
			}
			currentTime, currentValue := iter.At()
			currentDataPoint := &DataPoint{Time: currentTime, Value: currentValue}

			if lastDataPoint.Value >= 0 {
				// Note: We cast float to integer to eliminate the risk of a
				// precision error
				if !isValid(lastDataPoint, currentDataPoint) {
					t.Fatalf("The raw-data consistency check failed: metric name='%s'\n\tisValid(%v, %v) == false",
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

func PrefixTablePath(tablePath string) string {
	base := os.Getenv("TSDB_TEST_TABLE_PATH")
	if base == "" {
		return tablePath
	}
	return path.Join(os.Getenv("TSDB_TEST_TABLE_PATH"), tablePath)
}

func IteratorToSlice(it chunkenc.Iterator) ([]DataPoint, error) {
	var result []DataPoint
	for it.Next() {
		t, v := it.At()
		if it.Err() != nil {
			return nil, it.Err()
		}
		result = append(result, DataPoint{Time: t, Value: v})
	}
	return result, nil
}

func NanosToMillis(nanos int64) int64 {
	millis := nanos / int64(time.Millisecond)
	return millis
}
