package benchmark

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/test/benchmark/common"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
	"github.com/v3io/v3io-tsdb/config"
)

func BenchmarkIngest(b *testing.B) {
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	testStartTimeNano := time.Now().UnixNano()

	var count = 0 // count real number of samples to compare with query result

	testConfig, err := common.LoadBenchmarkIngestConfigFromData()
	if err != nil {
		panic(err)
	}

	v3ioConfigFile := os.Getenv(common.TsdbV3ioConfig)
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
	sampleTemplates := common.MakeSampleTemplates(
		common.MakeSamplesModel(
			testConfig.NamesCount,
			testConfig.NamesDiversity,
			testConfig.LabelsCount,
			testConfig.LabelsDiversity,
			testConfig.LabelValuesCount,
			testConfig.LabelsValueDiversity))

	for i := 0; i < b.N; i++ {
		count += runTest(appender, b, timeStamps, sampleTemplates, testConfig.FlushFrequency, testConfig.Verbose)
	}

	b.Logf("\nTest complete. Count: %d\n", count)
}

func runTest(
	appender tsdb.Appender,
	b *testing.B,
	timeStamps []int64,
	sampleTemplates []string,
	flushFrequency int,
	verbose bool) int {
	rowsAdded := appendAll(appender, b, sampleTemplates, timeStamps, flushFrequency, verbose)

	return rowsAdded
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
			sample := common.JsonTemplate2Sample(sampleTemplateJson, testCtx, initialTimeStamp, common.MakeRandomFloat64())
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
				sample := common.JsonTemplate2Sample(sampleTemplateJson, testCtx, timeStamps[dataPointIndex], common.MakeRandomFloat64())
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
