package benchmark

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"github.com/v3io/v3io-tsdb/test/benchmark/common"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

func BenchmarkIngest(b *testing.B) {
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	testStartTimeNano := time.Now().UnixNano()

	var count = 0 // count real number of samples to compare with query result

	testConfig, v3ioConfig, err := common.LoadBenchmarkIngestConfigs()
	if err != nil {
		panic(errors.Wrap(err, "Unable to load configuration"))
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

	sampleTemplates := common.MakeSampleTemplates(
		common.MakeSamplesModel(
			testConfig.NamesCount,
			testConfig.NamesDiversity,
			testConfig.LabelsCount,
			testConfig.LabelsDiversity,
			testConfig.LabelValuesCount,
			testConfig.LabelsValueDiversity))

	samplesCount := len(sampleTemplates)
	refsMap := make(map[uint64]bool, samplesCount)
	refsArray := make([]uint64, samplesCount)

	for i := 0; i < b.N; i++ {
		rowsAdded := runTest(i, appender, b, timeStamps, sampleTemplates, testConfig.FlushFrequency, refsMap, refsArray,
			testConfig.AppendOneByOne, testConfig.Verbose)
		if rowsAdded == 0 {
			break // stop the test (target has been achieved)
		}
		count += rowsAdded
	}

	b.Logf("\nTest complete. Count: %d\n", count)
}

func runTest(
	index int,
	appender tsdb.Appender,
	testCtx *testing.B,
	timeStamps []int64,
	sampleTemplates []string,
	flushFrequency int,
	refsMap map[uint64]bool,
	refsArray []uint64,
	appendOneByOne bool,
	verbose bool) int {

	samplesCount := len(sampleTemplates)
	tsCount := len(timeStamps)

	if samplesCount > 0 && tsCount > 0 {
		if appendOneByOne {
			return appendSingle(index%samplesCount, index/samplesCount, appender, testCtx, sampleTemplates[index%samplesCount],
				timeStamps[index/samplesCount], refsMap, refsArray)
		} else {
			return appendAll(appender, testCtx, sampleTemplates, timeStamps, flushFrequency, refsMap, refsArray, verbose)
		}
		// Wait for all responses
		waitForWrites(appender, &refsMap, testCtx)
	} else {
		testCtx.Fatalf("Insufficient input. "+
			"Samples count: [%d] and timestamps count [%d] should be positive numbers", samplesCount, tsCount)
	}
	return 0
}

func appendSingle(refIndex, cycleId int, appender tsdb.Appender, testCtx *testing.B, sampleTemplateJson string,
	timeStamp int64, refsMap map[uint64]bool, refsArray []uint64) int {
	count := 0
	sample := common.JsonTemplate2Sample(sampleTemplateJson, testCtx, timeStamp, common.MakeRandomFloat64())

	if cycleId == 0 {
		// initialize refIds
		// Add first & get reference
		ref, err := appender.Add(sample.Lset, timeStamp, sample.Value)
		if err != nil {
			testCtx.Fatalf("Add request has failed!\nError: %s", err)
		}
		refsArray[refIndex] = ref
		refsMap[ref] = true
		count++
	} else {
		// use refIds and AddFast
		err := appender.AddFast(sample.Lset, refsArray[refIndex], timeStamp, sample.Value)
		if err != nil {
			testCtx.Fatalf("AddFast request has failed!\nSample:%v\nError: %s", sample, err)
		}
		count++
	}

	return count
}

func appendAll(appender tsdb.Appender, testCtx *testing.B, sampleTemplates []string, timeStamps []int64,
	flushFrequency int, refsMap map[uint64]bool, refsArray []uint64, verbose bool) int {
	count := 0

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
