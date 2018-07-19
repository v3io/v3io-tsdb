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
		b.Fatal(errors.Wrap(err, "unable to load configuration"))
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
		b.Fatal("unable to resolve start time. Check configuration.")
	}
	testStartTimeMs := testStartTimeNano/int64(time.Millisecond) - relativeTimeOffsetMs
	timestampsCount := (testStartTimeNano/int64(time.Millisecond) - testStartTimeMs) / int64(testConfig.SampleStepSize)
	timestamps := make([]int64, timestampsCount)

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
	refsMap := make(map[uint64]bool, samplesCount)
	refs := make([]uint64, samplesCount)

	for i := 0; i < b.N; i++ {
		rowsAdded, err := runTest(i, appender, timestamps, sampleTemplates, testConfig.FlushFrequency, refsMap, refs,
			testConfig.AppendOneByOne, testConfig.Verbose)

		if err != nil {
			b.Fatal(err)
		}

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
	timestamps []int64,
	sampleTemplates []string,
	flushFrequency int,
	refsMap map[uint64]bool,
	refs []uint64,
	appendOneByOne bool,
	verbose bool) (int, error) {

	samplesCount := len(sampleTemplates)
	tsCount := len(timestamps)

	count := 0
	var err error
	if samplesCount > 0 && tsCount > 0 {
		if appendOneByOne {
			count, err = appendSingle(index%samplesCount, index/samplesCount, appender, sampleTemplates[index%samplesCount],
				timestamps[index/samplesCount], refsMap, refs)
		} else {
			count, err = appendAll(appender, sampleTemplates, timestamps, flushFrequency, refsMap, refs, verbose)
		}
	} else {
		err = errors.Errorf("insufficient input. "+
			"Samples count: [%d] and timestamps count [%d] should be positive numbers", samplesCount, tsCount)
	}
	return count, err
}

func appendSingle(refIndex, cycleId int, appender tsdb.Appender, sampleTemplateJson string,
	timestamp int64, refsMap map[uint64]bool, refs []uint64) (int, error) {
	sample, err := common.JsonTemplate2Sample(sampleTemplateJson, timestamp, common.MakeRandomFloat64())
	if err != nil {
		return 0, errors.Wrap(err, fmt.Sprintf("unable to unmarshall sample: %s", sampleTemplateJson))
	}

	if cycleId == 0 {
		// initialize refIds
		// Add first & get reference
		ref, err := appender.Add(sample.Lset, timestamp, sample.Value)
		if err != nil {
			return 0, errors.Wrap(err, "Add request has failed!")
		}
		refs[refIndex] = ref
		refsMap[ref] = true
	} else {
		err := appender.AddFast(sample.Lset, refs[refIndex], timestamp, sample.Value)
		if err != nil {
			return 0, errors.Wrap(err, fmt.Sprintf("AddFast request has failed!\nSample:%v", sample))
		}
	}

	return 1, nil
}

func appendAll(appender tsdb.Appender, sampleTemplates []string, timestamps []int64,
	flushFrequency int, refsMap map[uint64]bool, refs []uint64, verbose bool) (int, error) {
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
		refsMap[ref] = true
		count++
	}

	timeSeriesSize := len(timestamps)
	for dataPointIndex := 1; dataPointIndex < timeSeriesSize; dataPointIndex++ {
		for refIndex, sampleTemplateJson := range sampleTemplates {
			sample, err := common.JsonTemplate2Sample(sampleTemplateJson, timestamps[dataPointIndex], common.MakeRandomFloat64())
			if err != nil {
				return count, err
			}
			err = appender.AddFast(sample.Lset, refs[refIndex], timestamps[dataPointIndex], sample.Value)
			if err != nil {
				return count, errors.Wrap(err, fmt.Sprintf("AddFast request has failed! Sample:%v", sample))
			}
			count++

			if verbose || dataPointIndex%10 == 0 {
				fmt.Printf("\rTotal samples count: %d [%d %%]\tTime: %s",
					count,
					dataPointIndex*100/timeSeriesSize,
					time.Unix(int64(timestamps[dataPointIndex])/1000, 0).Format(time.RFC3339))
			}
		}

		if flushFrequency > 0 && dataPointIndex%flushFrequency == 0 {
			// block and flush all metrics every flush interval
			err := waitForWrites(appender, &refsMap)
			if err != nil {
				return count, err
			}
		}
	}

	// Wait for all responses
	err := waitForWrites(appender, &refsMap)

	return count, err
}

func waitForWrites(append tsdb.Appender, refMap *map[uint64]bool) error {
	for ref := range *refMap {
		err := append.WaitForReady(ref)
		if err != nil {
			return errors.Wrap(err, "waitForWrites has failed!")
		}
	}

	return nil
}
