package benchmark

import (
	"encoding/json"
	"fmt"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/nuclio/nuclio-test-go"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"github.com/v3io/v3io-tsdb/test/benchmark/common"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

const defaultDbName = "db0"

func BenchmarkIngestWithNuclio(b *testing.B) {
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	testStartTimeNano := time.Now().UnixNano()

	var count = 0 // count real number of samples to compare with query result

	testConfig, v3ioConfig, err := common.LoadBenchmarkIngestConfigs()
	if err != nil {
		b.Fatal(errors.Wrap(err, "unable to load configuration"))
	}

	data := nutest.DataBind{
		Name:      defaultDbName,
		Url:       v3ioConfig.V3ioUrl,
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

	// run the runTest function b.N times
	relativeTimeOffsetMs, err := utils.Str2duration(testConfig.StartTimeOffset)
	if err != nil {
		b.Fatal("unable to resolve start time. Check configuration.")
	}
	testStartTimeMs := testStartTimeNano/int64(time.Millisecond) - relativeTimeOffsetMs
	sampleTemplates := common.MakeSampleTemplates(
		common.MakeSamplesModel(
			testConfig.NamesCount,
			testConfig.NamesDiversity,
			testConfig.LabelsCount,
			testConfig.LabelsDiversity,
			testConfig.LabelValuesCount,
			testConfig.LabelsValueDiversity))
	sampleTemplatesLength := len(sampleTemplates)

	for i := 0; i < b.N; i++ {
		index := i % sampleTemplatesLength
		timestamp := testStartTimeMs + int64(index*testConfig.SampleStepSize)
		newEntries, err := runNuclioTest(tc, sampleTemplates[index], timestamp)
		if err != nil {
			b.Fatal(err)
		}
		count += newEntries
	}

	tc.Logger.Warn("\nTest complete. Count: %d", count)
}

func runNuclioTest(tc *nutest.TestContext, sampleTemplateJson string, timestamp int64) (int, error) {
	count := 0
	// Add first & get reference
	sampleJson := fmt.Sprintf(sampleTemplateJson, timestamp, common.MakeRandomFloat64())
	tc.Logger.Debug("Sample data: %s", sampleJson)

	testEvent := nutest.TestEvent{
		Body: []byte(sampleJson),
	}

	resp, err := tc.Invoke(&testEvent)

	if err != nil {
		errors.Wrap(err, fmt.Sprintf("request has failed. Response: %s\n", resp))
	}
	count++

	return count, err
}

// InitContext runs only once when the function runtime starts
func initContext(context *nuclio.Context) error {
	cfg, err := config.LoadConfig("")
	if err != nil {
		return err
	}

	data := context.DataBinding[defaultDbName].(*v3io.Container)
	adapter, err := tsdb.NewV3ioAdapter(cfg, data, context.Logger)
	if err != nil {
		return err
	}

	appender, err := adapter.Appender()
	if err != nil {
		return err
	}
	context.UserData = appender
	return nil
}

func handler(context *nuclio.Context, event nuclio.Event) (interface{}, error) {

	sample := common.Sample{}
	err := json.Unmarshal(event.GetBody(), &sample)
	if err != nil {
		return nil, err
	}
	app := context.UserData.(tsdb.Appender)

	// if time is not specified assume "now"
	if sample.Time == "" {
		sample.Time = "now"
	}

	// convert time string to time int, string can be: now, now-2h, int (unix milisec time), or RFC3339 date string
	t, err := utils.Str2unixTime(sample.Time)
	if err != nil {
		return "", err
	}

	// Append sample to metric
	_, err = app.Add(sample.Lset, t, sample.Value)

	return "", err
}
