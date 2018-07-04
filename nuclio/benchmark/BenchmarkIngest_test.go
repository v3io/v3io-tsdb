package benchmark

import (
	"fmt"
	"testing"
	"os"
	"time"
	"github.com/nuclio/nuclio-test-go"
	"math/rand"
	"io/ioutil"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/nuclio/ingest"
	"bytes"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"log"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const defaultDbName = "tsdb-test-01"

var count = 0 // count real number of samples to compare with query result

type BenchmarkRandomIngestConfig struct {
	V3ioUrl              string `json:"V3ioUrl,omitempty" yaml:"V3ioUrl"`
	Container            string `json:"Container,omitempty" yaml:"Container"`
	StartTimeOffset      string `json:"StartTimeOffset,omitempty" yaml:"StartTimeOffset"`
	SampleStepSize       int    `json:"SampleStepSize,omitempty" yaml:"SampleStepSize"`
	NamesCount           int    `json:"NamesCount,omitempty" yaml:"NamesCount"`
	NamesDiversity       int    `json:"NamesDiversity,omitempty" yaml:"NamesDiversity"`
	LabelsCount          int    `json:"LabelsCount,omitempty" yaml:"LabelsCount"`
	LabelsDiversity      int    `json:"LabelsDiversity,omitempty" yaml:"LabelsDiversity"`
	LabelValuesCount     int    `json:"LabelValuesCount,omitempty" yaml:"LabelValuesCount"`
	LabelsValueDiversity int    `json:"LabelsValueDiversity,omitempty" yaml:"LabelsValueDiversity"`
}

func loadFromData(data []byte) (*BenchmarkRandomIngestConfig, error) {
	cfg := BenchmarkRandomIngestConfig{}
	err := yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	initDefaults(&cfg)

	return &cfg, err
}

func initDefaults(cfg *BenchmarkRandomIngestConfig) {
	if cfg.V3ioUrl == "" {
		var v3ioUrl = os.Getenv("V3IO_URL")
		if v3ioUrl == "" {
			v3ioUrl = "localhost:8081"
		}
		cfg.V3ioUrl = v3ioUrl
	}

	if cfg.Container == "" {
		cfg.Container = "bigdata"
	}

	if cfg.StartTimeOffset == "" {
		cfg.StartTimeOffset = "48h"
	}
}

func BenchmarkRandomIngest(b *testing.B) {
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	testStartTimeNano := time.Now().UnixNano()

	var benchConfigFile = os.Getenv("TSDB_BENCH_RANDOM_INGEST_CONFIG")
	if benchConfigFile == "" {
		benchConfigFile = "tsdb-bench-test-config.yaml"
	}

	configData, err := ioutil.ReadFile(benchConfigFile)
	if err != nil {
		log.Fatal(err)
		os.Exit(2)
	}

	testConfig, err := loadFromData(configData)
	if err != nil {
		// if we couldn't load the file and its not the default
		if benchConfigFile != "" {
			panic(errors.Wrap(err, fmt.Sprintf("Failed to load config from file %s", benchConfigFile)))
		}
	}

	v3ioConfigFile := os.Getenv("V3IO_TSDBCFG_PATH")
	v3ioConfig, err := config.LoadConfig(v3ioConfigFile)
	if err != nil {
		panic(errors.Wrap(err, fmt.Sprintf("Failed to load config from file %s", v3ioConfigFile)))
	}
	data := nutest.DataBind{
		Name:      defaultDbName,
		Url:       testConfig.V3ioUrl,
		Container: testConfig.Container,
		User:      v3ioConfig.Username,
		Password:  v3ioConfig.Password,
	}

	tc, err := nutest.NewTestContext(ingest.Handler, true, &data)
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
		b.Fatal("Unable to resolve start time. Check configuration.")
	}
	testStartTimeMs := testStartTimeNano/int64(time.Millisecond) - relativeTimeOffsetMs

	for i := 0; i < b.N; i++ {
		runTest(i, tc, b, testConfig, testStartTimeMs)
	}

	tc.Logger.Warn("Test complete. Count: %d", count)
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

func runTest(i int, tc *nutest.TestContext, b *testing.B, cfg *BenchmarkRandomIngestConfig, testStartTimeMs int64) {
	sampleTimeMs := testStartTimeMs + int64(i*cfg.SampleStepSize)
	samples := makeSamples(
		cfg.NamesCount,
		cfg.NamesDiversity,
		cfg.LabelsCount,
		cfg.LabelsDiversity,
		cfg.LabelValuesCount,
		cfg.LabelsValueDiversity,
		sampleTimeMs)

	for _, sample := range *samples {
		tc.Logger.Debug("Sample data: %s", sample)

		testEvent := nutest.TestEvent{
			Body: []byte(sample),
		}

		resp, err := tc.Invoke(&testEvent)

		if err != nil {
			b.Fatalf("Request has failed!\nError: %s\nResponse: %s\n", err, resp)
		}
		count ++
	}
}

func makeSamples(namesCount, namesDiversity, labelsCount, labelDiversity, labelValueCount,
labelValueDiversity int, timeStamp int64) *[] string {
	names, err := makeNamesRange("Name", namesCount, 1, namesDiversity)
	if err != nil {
		panic(err)
	}
	labels, err := makeNamesRange("Label", labelsCount, 1, labelDiversity)
	if err != nil {
		panic(err)
	}

	sizeEstimate := namesCount * namesDiversity * labelsCount * labelDiversity
	model := make(map[string]map[string][]string, sizeEstimate) // names -> labels -> label values

	for _, name := range names {
		model[name] = make(map[string][]string)
		for _, label := range labels {
			model[name][label] = []string{}
			labelValues, err := makeNamesRange("", labelValueCount, 1, labelValueDiversity)
			if err != nil {
				panic(err)
			}
			for _, labelValues := range labelValues {
				model[name][label] = append(model[name][label], labelValues)
			}
		}
	}

	samples := makeSampleTemplates(model)

	for i, sample := range *samples {
		rand.Seed(time.Now().UnixNano())
		(*samples)[i] = fmt.Sprintf(sample, timeStamp, rand.Float64()*100)
	}

	return samples
}

func makeSampleTemplates(model map[string]map[string][]string) *[]string {
	var result = make([]string, 0)
	for name, labels := range model {
		valSetLenght := 0
		// TODO: find better way to get size of the label values array
		for _, labelValues := range labels {
			valSetLenght = len(labelValues)
			break
		}
		for index := 0; index < valSetLenght; index++ {
			var buffer bytes.Buffer
			buffer.WriteString(fmt.Sprintf("{\"Lset\": { \"__name__\":\"%s\"", name))
			for label, labelValues := range labels {
				buffer.WriteString(", \"")
				buffer.WriteString(label)
				buffer.WriteString(fmt.Sprintf("\" : \"%s\"", labelValues[index]))
			}
			buffer.WriteString("}, \"Time\" : %d, \"Value\" : %f}")
			result = append(result, buffer.String())
		}
	}

	return &result
}

func makeNamesRange(prefix string, count, minIndex, maxIndex int) ([]string, error) {
	if count < 1 {
		return nil, fmt.Errorf("makeNamesRange(%s): Minimal count is 1 but actual is %d", prefix, count)
	}

	if count > 26 {
		// Maximal range contains 26 elements, i.e. <prefix>_[A to Z]
		return nil, fmt.Errorf("makeNamesRange(%s): Maximal number of names is 26 [A..Z], but actual number is %d", prefix, count)
	}

	normalizedPrefix := prefix
	if len(prefix) > 0 {
		normalizedPrefix = fmt.Sprintf("%s_", prefix)
	}

	limit := maxIndex - minIndex + 1
	size := 1
	if count == 1 {
		size = limit
	} else {
		size = count * limit
	}
	array := make([]string, size)

	for i := range array {
		if limit > 1 {
			array[i] = fmt.Sprintf("%s%s_%d", normalizedPrefix, string(65+(i/limit)%count), minIndex+i%limit)
		} else {
			array[i] = fmt.Sprintf("%s%s", normalizedPrefix, string(65+(i/limit)%count))
		}
	}
	return array, nil
}
