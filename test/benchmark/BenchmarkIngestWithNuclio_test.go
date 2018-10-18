package benchmark

import (
	"encoding/json"
	"fmt"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/nuclio/nuclio-test-go"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest/testutils"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"github.com/v3io/v3io-tsdb/test/benchmark/common"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

const defaultDbName = "db0"

// Hack: Define GLOBAL variables to get access to private stuff
var v3ioAdapter *tsdb.V3ioAdapter
var tsdbPath string

func BenchmarkIngestWithNuclio(b *testing.B) {
	b.StopTimer()
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	testStartTimeNano := time.Now().UnixNano()

	var count = 0 // Count of real number of samples to compare with the query result

	testConfig, v3ioConfig, err := common.LoadBenchmarkIngestConfigs()
	if err != nil {
		b.Fatal(errors.Wrap(err, "unable to load configuration"))
	}

	// Create a test TSDB instance (table) path)
	tsdbPath = tsdbtest.PrefixTablePath(tsdbtest.NormalizePath(fmt.Sprintf("tsdb-%s-%d-%s", b.Name(), b.N, time.Now().Format(time.RFC3339))))

	// Update TSDB instance path for this test
	v3ioConfig.TablePath = tsdbPath
	schema := testutils.CreateSchema(b, "count,sum")
	if err := tsdb.CreateTSDB(v3ioConfig, schema); err != nil {
		b.Fatal("Failed to create TSDB", err)
	}

	data := nutest.DataBind{
		Name:      defaultDbName,
		Url:       v3ioConfig.WebApiEndpoint,
		Container: v3ioConfig.Container,
		User:      v3ioConfig.Username,
		Password:  v3ioConfig.Password,
	}

	tc, err := nutest.NewTestContext(handler, testConfig.Verbose, &data)
	if err != nil {
		b.Fatal(err)
	}

	err = tc.InitContext(initContext)
	if err != nil {
		b.Fatal(err)
	}

	// Run the runTest function b.N times
	relativeTimeOffsetMs, err := utils.Str2duration(testConfig.StartTimeOffset)
	if err != nil {
		b.Fatal("Unable to resolve start time. Check the configuration.")
	}
	testEndTimeMs := testStartTimeNano / int64(time.Millisecond)
	testStartTimeMs := testEndTimeMs - relativeTimeOffsetMs

	samplesModel := common.MakeSamplesModel(
		testConfig.NamesCount,
		testConfig.NamesDiversity,
		testConfig.LabelsCount,
		testConfig.LabelsDiversity,
		testConfig.LabelValuesCount,
		testConfig.LabelsValueDiversity)
	sampleTemplates := common.MakeSampleTemplates(samplesModel)
	sampleTemplatesLength := len(sampleTemplates)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		index := i % sampleTemplatesLength
		timestamp := testStartTimeMs + int64(index*testConfig.SampleStepSize)
		newEntries, err := runNuclioTest(tc, sampleTemplates[index], timestamp, testConfig.ValidateRawData)
		if err != nil {
			b.Fatal(err)
		}
		count += newEntries
	}

	appender, err := v3ioAdapter.Appender()
	if err != nil {
		b.Error(err)
	}
	// Wait for all responses; use the configured timeout or the default unlimited timeout.
	_, err = appender.WaitForCompletion(-1)
	b.StopTimer()

	tc.Logger.Warn("\nTest complete. Count: %d", count)

	queryStepSizeMs, err := utils.Str2duration(testConfig.QueryAggregateStep)
	if err != nil {
		b.Fatal("Unable to resolve the query aggregate interval (step) size. Check the configuration.")
	}

	tsdbtest.ValidateCountOfSamples(b, v3ioAdapter, metricNamePrefix, count, testStartTimeMs, testEndTimeMs, queryStepSizeMs)

	if testConfig.ValidateRawData {
		for metricName := range samplesModel {
			tsdbtest.ValidateRawData(b, v3ioAdapter, metricName, testStartTimeMs, testEndTimeMs, isValidDataPoint)
		}
	}
}

func runNuclioTest(tc *nutest.TestContext, sampleTemplateJson string, timestamp int64, sequential bool) (int, error) {
	count := 0
	// Add first & get reference
	sampleJson := fmt.Sprintf(sampleTemplateJson, timestamp, common.NextValue(sequential))
	tc.Logger.Debug("Sample data: %s", sampleJson)

	testEvent := nutest.TestEvent{
		Body: []byte(sampleJson),
	}

	resp, err := tc.Invoke(&testEvent)

	if err != nil {
		errors.Wrap(err, fmt.Sprintf("The request failed. Response: %s\n", resp))
	}
	count++

	return count, err
}

// InitContext runs only once, when the function runtime starts
func initContext(context *nuclio.Context) error {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		return errors.Wrap(err, "failed to load configuration")
	}

	// Hack - update path to TSDB
	v3ioConfig.TablePath = tsdbPath

	data := context.DataBinding[defaultDbName].(*v3io.Container)
	adapter, err := tsdb.NewV3ioAdapter(v3ioConfig, data, context.Logger)
	if err != nil {
		return errors.Wrap(err, "failed to create V3IO Adapter")
	}

	// Store the adapter in the user-data cache
	context.UserData = adapter

	v3ioAdapter = adapter

	return nil
}

func handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {

	sample := tsdbtest.Sample{}
	err := json.Unmarshal(event.GetBody(), &sample)
	if err != nil {
		return nil, err
	}

	adapter := context.UserData.(*tsdb.V3ioAdapter)

	appender, err := adapter.Appender()
	if err != nil {
		return "", err
	}

	// If time isn't specified, assume "now" (default)
	if sample.Time == "" {
		sample.Time = "now"
	}

	// Convert a time string to a Unix timestamp in milliseconds integer.
	// The input time string can be of the format "now", "now-[0-9]+[mdh]"
	// (for example, "now-2h"), "<Unix timestamp in milliseconds>", or
	// "<RFC 3339 time>" (for example, "2018-09-26T14:10:20Z").
	t, err := utils.Str2unixTime(sample.Time)
	if err != nil {
		return "", err
	}

	// Append a sample to a metric-samples set
	_, err = appender.Add(sample.Lset, t, sample.Value)

	return "", err
}
