package benchmark

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

var count = 0 // count real number of samples to compare with query result

type BenchmarkIngestConfig struct {
	Verbose              bool   `json:"Verbose,omitempty" yaml:"Verbose"`
	StartTimeOffset      string `json:"StartTimeOffset,omitempty" yaml:"StartTimeOffset"`
	SampleStepSize       int    `json:"SampleStepSize,omitempty" yaml:"SampleStepSize"`
	NamesCount           int    `json:"NamesCount,omitempty" yaml:"NamesCount"`
	NamesDiversity       int    `json:"NamesDiversity,omitempty" yaml:"NamesDiversity"`
	LabelsCount          int    `json:"LabelsCount,omitempty" yaml:"LabelsCount"`
	LabelsDiversity      int    `json:"LabelsDiversity,omitempty" yaml:"LabelsDiversity"`
	LabelValuesCount     int    `json:"LabelValuesCount,omitempty" yaml:"LabelValuesCount"`
	LabelsValueDiversity int    `json:"LabelsValueDiversity,omitempty" yaml:"LabelsValueDiversity"`
	FlushFrequency       int    `json:"FlushFrequency,omitempty" yaml:"FlushFrequency"`
}

func loadFromData(data []byte) (*BenchmarkIngestConfig, error) {
	cfg := BenchmarkIngestConfig{}
	err := yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	initDefaults(&cfg)

	return &cfg, err
}

func initDefaults(cfg *BenchmarkIngestConfig) {
	if cfg.StartTimeOffset == "" {
		cfg.StartTimeOffset = "48h"
	}
}

func BenchmarkIngest(b *testing.B) {
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

	adapter, err := tsdb.NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		b.Fatal(err)
	}

	// run the runTest function b.N times
	relativeTimeOffsetMs, err := utils.Str2duration(testConfig.StartTimeOffset)
	if err != nil {
		b.Fatal("Unable to resolve start time. Check configuration.")
	}
	testStartTimeMs := testStartTimeNano/int64(time.Millisecond) - relativeTimeOffsetMs
	timeStampsCount := (testStartTimeNano/int64(time.Millisecond) - testStartTimeMs) / int64(testConfig.SampleStepSize)
	timeStamps := make([]int64, timeStampsCount)

	for i := range timeStamps {
		timeStamps[i] = testStartTimeMs + int64(i*testConfig.SampleStepSize)
	}

	//refMap := map[uint64]bool{}
	sampleTemplates := makeSampleTemplates(
		makeSamplesModel(
			testConfig.NamesCount,
			testConfig.NamesDiversity,
			testConfig.LabelsCount,
			testConfig.LabelsDiversity,
			testConfig.LabelValuesCount,
			testConfig.LabelsValueDiversity))

	for i := 0; i < b.N; i++ {
		runTest(appender, b, timeStamps, sampleTemplates, testConfig.FlushFrequency, testConfig.Verbose)
	}

	b.Logf("\nTest complete. Count: %d\n", count)
}

func runTest(
	appender tsdb.Appender,
	b *testing.B,
	timeStamps []int64,
	sampleTemplates []string,
	flushFrequency int,
	verbose bool) {
	rowsAdded := appendAll(appender, b, sampleTemplates, timeStamps, flushFrequency, verbose)

	count = count + rowsAdded
}

func makeSamplesModel(namesCount, namesDiversity, labelsCount, labelDiversity, labelValueCount,
labelValueDiversity int) map[string]map[string][]string {
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
	return model
}

func makeSampleTemplates(model map[string]map[string][]string) []string {
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
			buffer.WriteString("}, \"Time\" : \"%d\", \"Value\" : %.2f}")
			result = append(result, buffer.String())
		}
	}

	return result
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

func appendAll(appender tsdb.Appender, testCtx *testing.B, sampleTemplates []string, timeStamps []int64,
	flushFrequency int, verbose bool) int {
	count := 0

	samplesCount := len(sampleTemplates)
	tsCount := len(timeStamps)

	if samplesCount > 0 && tsCount > 0 {
		refsMap := map[uint64]bool{}
		refsArray := make([]uint64, len(sampleTemplates))
		// First pass - populate references for add fast
		initialTimeStamp := timeStamps[0]
		for i, sampleTemplateJson := range sampleTemplates {
			// Add first & get reference
			sample := jsonTemplate2Sample(sampleTemplateJson, testCtx, initialTimeStamp, makeRandomValue())
			ref, err := appender.Add(sample.Lset, initialTimeStamp, sample.Value)
			if err != nil {
				testCtx.Fatalf("Add request has failed!\nError: %s", err)
			}
			refsArray[i] = ref
			refsMap[ref] = true
			count++
		}

		timeSerieSize := len(timeStamps)
		for dataPointIndex := 1; dataPointIndex < timeSerieSize; dataPointIndex++ {
			for refIndex, sampleTemplateJson := range sampleTemplates {
				sample := jsonTemplate2Sample(sampleTemplateJson, testCtx, timeStamps[dataPointIndex], makeRandomValue())
				err := appender.AddFast(sample.Lset, refsArray[refIndex], timeStamps[dataPointIndex], sample.Value)
				if err != nil {
					testCtx.Fatalf("AddFast request has failed!\nSample:%v\nError: %s", sample, err)
				}
				count++

				if verbose || dataPointIndex%10 == 0 {
					fmt.Printf("\rTotal samples count: %d [%d %%]\tTime: %s",
						count,
						dataPointIndex*100/timeSerieSize,
						time.Unix(int64(timeStamps[dataPointIndex])/1000, 0).Format(time.RFC3339))
				}
			}

			if flushFrequency > 0 && dataPointIndex%flushFrequency == 0 {
				// block and flush all metrics every flush interval
				waitForWrites(appender, &refsMap, testCtx)
			}
		}
		waitForWrites(appender, &refsMap, testCtx)
	} else {
		testCtx.Fatalf("Insufficient input. "+
			"Samples count: [%d] and timestamps count [%d] should be positive numbers", samplesCount, tsCount)
	}

	return count
}

func waitForWrites(append tsdb.Appender, refMap *map[uint64]bool, testCtx *testing.B) {
	for ref := range *refMap {
		err := append.WaitForReady(ref)
		if err != nil {
			testCtx.Fatalf("waitForWrites has failed!\nError: %s", err)
		}
	}
}

func makeRandomValue() float64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Float64() * 100
}

func jsonTemplate2Sample(sampleJsonTemplate string, testCtx *testing.B, time int64, value float64) *sample {
	sampleJson := fmt.Sprintf(sampleJsonTemplate, time, value)
	sample, err := unmarshallSample(sampleJson)
	if err != nil {
		testCtx.Fatalf("Failed to unmarshall sample: %s\nError: %s", sampleJson, err)
	}

	return sample
}

func unmarshallSample(sampleJsonString string) (*sample, error) {
	sample := sample{}
	err := json.Unmarshal([]byte(sampleJsonString), &sample)
	if err != nil {
		return nil, err
	}

	return &sample, nil
}

type sample struct {
	Lset  utils.Labels
	Time  string
	Value float64
}
