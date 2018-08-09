package benchmark

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"github.com/v3io/v3io-tsdb/test/benchmark/common"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

const metricNamePrefix = "Name_"

func BenchmarkIngest(b *testing.B) {
	b.StopTimer()
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	testStartTimeNano := time.Now().UnixNano()

	var count = 0 // count real number of samples to compare with query result

	testConfig, v3ioConfig, err := common.LoadBenchmarkIngestConfigs()
	if err != nil {
		b.Fatal(errors.Wrap(err, "unable to load configuration"))
	}

	// Create test path (tsdb instance)
	tsdbPath := tsdbtest.NormalizePath(fmt.Sprintf("tsdb-%s-%d-%s", b.Name(), b.N, time.Now().Format(time.RFC3339)))

	// Update TSDB instance path for this test
	v3ioConfig.Path = tsdbPath
	schema := tsdbtest.CreateSchema(b, "*")
	if err := tsdb.CreateTSDB(v3ioConfig, &schema); err != nil {
		b.Fatal("Failed to create TSDB", err)
	}

	adapter, err := tsdb.NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	if testConfig.CleanupAfterTest {
		defer adapter.DeleteDB(true, true, 0, 0)
	}

	appender, err := adapter.Appender()
	if err != nil {
		b.Fatal(err)
	}

	// run the runTest function b.N times
	relativeTimeOffsetMs, err := utils.Str2duration(testConfig.StartTimeOffset)
	if err != nil {
		b.Fatal("unable to resolve start time. Check configuration.")
	}
	testEndTimeMs := testStartTimeNano / int64(time.Millisecond)
	testStartTimeMs := testEndTimeMs - relativeTimeOffsetMs
	timestampsCount := (testEndTimeMs - testStartTimeMs) / int64(testConfig.SampleStepSize)
	timestamps := make([]int64, timestampsCount)

	testStartTime := time.Unix(int64(testStartTimeMs/1000), 0).Format(time.RFC3339)
	testEndTime := time.Unix(int64(testEndTimeMs/1000), 0).Format(time.RFC3339)
	fmt.Printf("\nAbout to run %d ingestion cycles from %s [%d] to %s [%d]. Max samples count: %d\n",
		b.N,
		testStartTime, testStartTimeMs,
		testEndTime, testEndTimeMs,
		timestampsCount)

	for i := range timestamps {
		timestamps[i] = testStartTimeMs + int64(i*testConfig.SampleStepSize)
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
	refs := make([]uint64, samplesCount)
	testLimit := samplesCount * int(timestampsCount)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		rowsAdded, err := runTest(i, appender, timestamps, sampleTemplates, refs,
			testConfig.AppendOneByOne, testConfig.BatchSize, testConfig.Verbose)

		if err != nil {
			b.Fatal(err)
		}

		count += rowsAdded

		if rowsAdded == 0 || rowsAdded >= testLimit {
			defer b.Skipf("\nTest have reached the target (limit=%d)", testLimit)
			break // stop the test (target has been achieved)
		}
	}

	// Wait for all responses, use default timeout from configuration or unlimited if not set
	_, err = appender.WaitForCompletion(-1)
	b.StopTimer()

	if err != nil {
		b.Fatalf("Test timed out. Error: %v", err)
	}

	b.Logf("\nTest complete. %d samples added to %s\n", count, tsdbPath)

	tsdbtest.ValidateCountOfSamples(b, adapter, metricNamePrefix, count, testStartTimeMs, testEndTimeMs)
}

func runTest(
	index int,
	appender tsdb.Appender,
	timestamps []int64,
	sampleTemplates []string,
	refs []uint64,
	appendOneByOne bool,
	batchSize int,
	verbose bool) (int, error) {

	samplesCount := len(sampleTemplates)
	tsCount := len(timestamps)

	count := 0
	var err error
	if samplesCount > 0 && tsCount > 0 {
		if appendOneByOne {
			startFromIndex := (index / samplesCount) * batchSize
			endIndex := min(startFromIndex+batchSize, tsCount)
			if startFromIndex < tsCount {
				batchOfTimestamps := timestamps[startFromIndex:endIndex]
				count, err = appendSingle(index%samplesCount, index/samplesCount, appender, sampleTemplates[index%samplesCount],
					batchOfTimestamps, refs)
			} else {
				// Test complete - filled the given time interval with samples
				fmt.Printf("Breaking the loop with %d enties in range [%d:%d]", endIndex-startFromIndex, startFromIndex, endIndex)
				return count, nil
			}
		} else {
			count, err = appendAll(appender, sampleTemplates, timestamps, refs, batchSize, verbose)
		}
	} else {
		err = errors.Errorf("insufficient input. "+
			"Samples count: [%d] and timestamps count [%d] should be positive numbers", samplesCount, tsCount)
	}
	return count, err
}

func min(left, right int) int {
	if left <= right {
		return left
	}
	return right
}

func appendSingle(refIndex, cycleId int, appender tsdb.Appender, sampleTemplateJson string,
	timestamps []int64, refs []uint64) (int, error) {

	timestampIndex := 0
	sample, err := common.JsonTemplate2Sample(sampleTemplateJson, timestamps[timestampIndex], common.MakeRandomFloat64())
	if err != nil {
		return 0, errors.Wrap(err, fmt.Sprintf("unable to unmarshall sample: %s", sampleTemplateJson))
	}

	for timestamp := timestamps[timestampIndex]; timestampIndex < len(timestamps); timestampIndex++ {
		if cycleId == 0 && timestampIndex == 0 {
			// initialize refIds
			// Add first & get reference
			ref, err := appender.Add(sample.Lset, timestamp, sample.Value)
			if err != nil {
				return 0, errors.Wrap(err, "Add request has failed!")
			}
			refs[refIndex] = ref
		} else {
			err := appender.AddFast(sample.Lset, refs[refIndex], timestamp, sample.Value)
			if err != nil {
				return 0, errors.Wrap(err, fmt.Sprintf("AddFast request has failed!\nSample:%v", sample))
			}
		}
	}

	return timestampIndex, nil
}

func appendAll(appender tsdb.Appender, sampleTemplates []string, timestamps []int64,
	refs []uint64, batchSize int, verbose bool) (int, error) {
	count := 0

	// First pass - populate references for add fast
	initialTimeStamp := timestamps[0]
	for i, sampleTemplateJson := range sampleTemplates {
		// Add first & get reference
		sample, err := common.JsonTemplate2Sample(sampleTemplateJson, initialTimeStamp, common.MakeRandomFloat64())
		if err != nil {
			return count, errors.Wrap(err, fmt.Sprintf("unable to unmarshall sample: %s", sampleTemplateJson))
		}
		ref, err := appender.Add(sample.Lset, initialTimeStamp, sample.Value)
		if err != nil {
			return count, errors.Wrap(err, "Add request has failed!")
		}
		refs[i] = ref
		count++
	}

	timeSeriesSize := len(timestamps)
	for dataPointStartIndex := 1; dataPointStartIndex < timeSeriesSize; {
		// calculate batch boundaries
		remaining := timeSeriesSize - dataPointStartIndex
		actualBatchSize := min(remaining, batchSize)

		for refIndex, sampleTemplateJson := range sampleTemplates {
			for i := 0; i < actualBatchSize; i++ {
				sample, err := common.JsonTemplate2Sample(sampleTemplateJson, timestamps[dataPointStartIndex+i], common.MakeRandomFloat64())
				if err != nil {
					return count, err
				}
				err = appender.AddFast(sample.Lset, refs[refIndex], timestamps[dataPointStartIndex], sample.Value)
				if err != nil {
					return count, errors.Wrap(err, fmt.Sprintf("AddFast request has failed! Sample:%v", sample))
				}
				count++

				if verbose || dataPointStartIndex%10 == 0 {
					fmt.Printf("\rTotal samples count: %d [%d %%]\tTime: %s",
						count,
						dataPointStartIndex*100/timeSeriesSize,
						time.Unix(int64(timestamps[dataPointStartIndex])/1000, 0).Format(time.RFC3339))
				}
			}
		}
		dataPointStartIndex += actualBatchSize
	}

	// Wait for all responses, use default timeout from configuration or unlimited if not set
	_, err := appender.WaitForCompletion(-1)

	return count, err
}
