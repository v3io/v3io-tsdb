// +build integration

/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package aggregates

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-tsdb/internal/pkg/performance"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math"
	"testing"
	"time"
)

type TestConfig struct {
	desc           string
	testEndTime    int64
	testStartTime  int64
	interval       int64
	testDuration   int64
	numMetrics     int
	numLabels      int
	values         []float64
	queryFunc      string
	queryStep      int64
	expectedMin    []float64
	expectedMax    []float64
	expectedCount  []int
	expectedSum    []float64
	expectedAvg    []float64
	expectFail     bool
	ignoreReason   string
	v3ioConfig     *config.V3ioConfig
	logger         logger.Logger
	tsdbAggregates string
	setup          func() (*tsdb.V3ioAdapter, error, func())
}

type metricContext struct {
	lset utils.Labels
	ref  uint64
}

func TestAggregates(t *testing.T) {
	// TODO: consider replacing with test suite
	testCases := []*TestConfig{
		t1Config(t),
		t2Config(t),
		t3Config(t),
	}

	for _, testConfig := range testCases {
		t.Logf("%s\n", testConfig.desc)
		t.Run(testConfig.desc, func(t *testing.T) {
			if testConfig.ignoreReason != "" {
				t.Skip(testConfig.ignoreReason)
			}
			testAggregatesCase(t, testConfig)
		})
	}

}

func t1Config(testCtx *testing.T) *TestConfig {
	t := time.Now()
	// Round the test time down to the closest hour to get predictable and consistent results
	currentRoundedTimeNano := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).UnixNano()

	// Note, test duration, suery step and interval between samples must be aligned (must divide without remainder)
	testDuration := int64(80 * time.Hour)
	queryStep := int64(time.Hour)
	interval := int64(10 * time.Minute)
	numOfBuckets := int(testDuration / queryStep)

	testAggregates := "min,max,count,sum,avg"
	testValues := []float64{-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6}

	tc := &TestConfig{
		desc:           fmt.Sprintf("Test with aggregates: Check [%s] with %v", testAggregates, testValues),
		testEndTime:    currentRoundedTimeNano,
		testStartTime:  currentRoundedTimeNano - testDuration,
		interval:       interval,
		testDuration:   testDuration,
		numMetrics:     1,
		numLabels:      1,
		values:         testValues,
		expectedCount:  make([]int, numOfBuckets),
		expectedMin:    makeAndInitFloatArray(math.Inf(1), numOfBuckets),
		expectedMax:    makeAndInitFloatArray(math.Inf(-1), numOfBuckets),
		expectedSum:    make([]float64, numOfBuckets),
		expectedAvg:    make([]float64, numOfBuckets),
		queryFunc:      testAggregates,
		queryStep:      queryStep,
		tsdbAggregates: testAggregates, // Create DB with all required aggregates
	}

	tc.setup = func() (*tsdb.V3ioAdapter, error, func()) {
		return setupFunc(testCtx, tc)
	}

	return tc
}

func t2Config(testCtx *testing.T) *TestConfig {
	t := time.Now()
	// Round the test time down to the closest hour to get predictable and consistent results
	currentRoundedTimeNano := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).UnixNano()

	// Note, test duration, suery step and interval between samples must be aligned (must divide without remainder)
	testDuration := int64(96 * time.Hour)
	queryStep := int64(time.Hour)
	interval := int64(10 * time.Minute)
	numOfBuckets := int(testDuration / queryStep)

	testAggregates := "min,max,count,sum,avg"
	testValues := []float64{-8, 0, 1, 2, 3, 4, 5, 6}

	tc := &TestConfig{
		desc:           fmt.Sprintf("Test with RAW data: Check [%s] with %v", testAggregates, testValues),
		testEndTime:    currentRoundedTimeNano,
		testStartTime:  currentRoundedTimeNano - testDuration,
		interval:       interval,
		testDuration:   testDuration,
		numMetrics:     1,
		numLabels:      1,
		values:         testValues,
		expectedCount:  make([]int, numOfBuckets),
		expectedMin:    makeAndInitFloatArray(math.Inf(1), numOfBuckets),
		expectedMax:    makeAndInitFloatArray(math.Inf(-1), numOfBuckets),
		expectedSum:    make([]float64, numOfBuckets),
		expectedAvg:    make([]float64, numOfBuckets),
		queryFunc:      testAggregates,
		queryStep:      queryStep,
		tsdbAggregates: "", // create DB without aggregates (use RAW data)
	}

	tc.setup = func() (*tsdb.V3ioAdapter, error, func()) {
		return setupFunc(testCtx, tc)
	}

	return tc
}

func t3Config(testCtx *testing.T) *TestConfig {
	t := time.Now()
	// Round the test time down to the closest hour to get predictable and consistent results
	currentRoundedTimeNano := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).UnixNano()

	// Note, test duration, suery step and interval between samples must be aligned (must divide without remainder)
	testDuration := int64(8 * time.Minute)
	queryStep := int64(time.Minute)
	interval := int64(20 * time.Second)
	numOfBuckets := int(testDuration / queryStep)

	testAggregates := "min,max,sum,count,avg"
	testValues := []float64{0.07, 0.07, 0.13, 0.07, 0.07, 0.20, 0.07, 0.07, 0.20, 0.07, 0.13, 0.13, 0.00, 0.07, 0.07, 0.00, 0.07, 0.13, 0.00, 0.07, 0.27, 0.07}

	tc := &TestConfig{
		desc:           fmt.Sprintf("Test with RAW data: Check [%s] with %v", testAggregates, testValues),
		testEndTime:    currentRoundedTimeNano,
		testStartTime:  currentRoundedTimeNano - testDuration,
		interval:       interval,
		testDuration:   testDuration,
		numMetrics:     1,
		numLabels:      1,
		values:         testValues,
		expectedCount:  make([]int, numOfBuckets),
		expectedMin:    makeAndInitFloatArray(math.Inf(1), numOfBuckets),
		expectedMax:    makeAndInitFloatArray(math.Inf(-1), numOfBuckets),
		expectedSum:    make([]float64, numOfBuckets),
		expectedAvg:    make([]float64, numOfBuckets),
		queryFunc:      testAggregates,
		queryStep:      queryStep,
		tsdbAggregates: "", // create DB without aggregates (use RAW data)
	}

	tc.setup = func() (*tsdb.V3ioAdapter, error, func()) {
		return setupFunc(testCtx, tc)
	}

	return tc
}

// Setup for test and return tear-down function
func setupFunc(testCtx *testing.T, testConfig *TestConfig) (*tsdb.V3ioAdapter, error, func()) {
	// setup:
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		return nil, err, func() {}
	}

	v3ioConfig.TablePath = tsdbtest.PrefixTablePath(fmt.Sprintf("%s-%d", testCtx.Name(), time.Now().Nanosecond()))
	tsdbtest.CreateTestTSDBWithAggregates(testCtx, v3ioConfig, testConfig.tsdbAggregates)

	adapter, err := tsdb.NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		testCtx.Fatalf("Test failed. Reason: %v", err)
	}

	// Measure performance
	metricReporter, err := performance.DefaultReporterInstance()
	if err != nil {
		testCtx.Fatalf("unable to initialize performance metrics reporter: %v", err)
	}
	metricReporter.Start()

	testConfig.logger = adapter.GetLogger(testCtx.Name())
	// generate data and set expectations
	generateData(testCtx, testConfig, adapter, testConfig.logger)

	return adapter, nil, func() {
		// Tear down:
		defer metricReporter.Stop()
		// Don't delete the table if the test has failed
		if !testCtx.Failed() {
			tsdbtest.DeleteTSDB(testCtx, v3ioConfig)
		}
	}
}

func testAggregatesCase(t *testing.T, testConfig *TestConfig) {
	// Setup & TearDown
	adapter, err, tearDown := testConfig.setup()
	defer tearDown()

	if err != nil {
		t.Fatalf("unable to load configuration. Error: %v", err)
	}

	startBefore := testConfig.testStartTime - testConfig.queryStep

	qry, err := adapter.Querier(nil, nanosToMillis(startBefore), nanosToMillis(testConfig.testEndTime))
	if err != nil {
		t.Fatal(err)
	}

	set, err := qry.Select("metric0", testConfig.queryFunc, nanosToMillis(testConfig.queryStep), "")
	if err != nil {
		t.Fatal(err)
	}

	numOfBuckets := int(testConfig.testDuration / testConfig.queryStep)
	actualMin := makeAndInitFloatArray(math.Inf(1), numOfBuckets)
	actualMax := makeAndInitFloatArray(math.Inf(-1), numOfBuckets)

	actualCount := make([]int, numOfBuckets)
	actualSum := makeAndInitFloatArray(0.0, numOfBuckets)
	actualAvg := makeAndInitFloatArray(0.0, numOfBuckets)

	for set.Next() {
		if set.Err() != nil {
			t.Fatal(set.Err())
		}

		series := set.At()
		lset := series.Labels()
		aggr := lset.Get(aggregate.AggregateLabel)
		testConfig.logger.Debug(fmt.Sprintf("\nLables: %v", lset))

		bucket := 0
		iter := series.Iterator()
		for iter.Next() {

			tm, v := iter.At()
			sTime := time.Unix(int64(tm/1000), 0).Format(time.RFC3339)

			if err != nil {
				t.Errorf("Unable to parse timestamp. Time: %d", tm)
			}

			msg := fmt.Sprintf("--> Aggregate: %s t=%d [%s], v=%f\n", aggr, tm, sTime, v)
			testConfig.logger.Debug(msg)

			switch aggr {
			case "min":
				actualMin[bucket] = v
			case "max":
				actualMax[bucket] = v
			case "count":
				actualCount[bucket] = int(v)
			case "sum":
				actualSum[bucket] = v
			case "avg":
				actualAvg[bucket] = v
			}

			bucket++
		}

		if iter.Err() != nil {
			t.Fatal(iter.Err())
		}
	}

	assert.ElementsMatch(t, testConfig.expectedMin, actualMin, "Minimal value is not as expected")
	assert.ElementsMatch(t, testConfig.expectedMax, actualMax, "Maximal value is not as expected")
	assert.ElementsMatch(t, testConfig.expectedCount, actualCount, "Count value is not as expected")
	assert.ElementsMatch(t, testConfig.expectedSum, actualSum, "Sum value is not as expected")
	assert.ElementsMatch(t, testConfig.expectedAvg, actualAvg, "Average value is not as expected")
}

func nanosToMillis(nanos int64) int64 {
	millis := nanos / int64(time.Millisecond)
	return millis
}

func generateData(t *testing.T, testConfig *TestConfig, adapter *tsdb.V3ioAdapter, logger logger.Logger) {
	var metrics []*metricContext
	for m := 0; m < testConfig.numMetrics; m++ {
		for l := 0; l < testConfig.numLabels; l++ {
			metrics = append(metrics, &metricContext{
				lset: utils.LabelsFromStrings(
					"__name__", fmt.Sprintf("metric%d", m), "label", fmt.Sprintf("lbl%d", l)),
			})
		}
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatal(err)
	}

	numTestValues := len(testConfig.values)
	numOfValuesPerStep := int(testConfig.queryStep / testConfig.interval)
	index := 0
	j := 0
	var curTime int64
	for curTime = testConfig.testStartTime; curTime < testConfig.testStartTime+testConfig.testDuration; curTime += testConfig.interval {
		v := testConfig.values[index]
		err := writeNext(appender, metrics, nanosToMillis(curTime), v)
		if err != nil {
			t.Fatal(err)
		}

		i := j / numOfValuesPerStep
		testConfig.expectedCount[i] = testConfig.expectedCount[i] + 1
		testConfig.expectedSum[i] = testConfig.expectedSum[i] + v
		testConfig.expectedMin[i] = math.Min(testConfig.expectedMin[i], v)
		testConfig.expectedMax[i] = math.Max(testConfig.expectedMax[i], v)
		testConfig.expectedAvg[i] = testConfig.expectedSum[i] / float64(testConfig.expectedCount[i])

		index = (index + 1) % numTestValues
		j++
	}

	logger.Debug(fmt.Sprintf("total samples written: %d", j))

	res, err := appender.WaitForCompletion(0)

	if err != nil {
		t.Fatal(err, "Wait for completion has failed", res, err)
	}

	return
}

func writeNext(app tsdb.Appender, metrics []*metricContext, t int64, v float64) error {
	for _, metric := range metrics {
		if metric.ref == 0 {
			ref, err := app.Add(metric.lset, t, v)
			if err != nil {
				return err
			}
			metric.ref = ref
		} else {
			err := app.AddFast(metric.lset, metric.ref, t, v)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func makeAndInitFloatArray(initWith float64, length int) []float64 {
	floats := make([]float64, length)
	for i := range floats {
		floats[i] = initWith
	}

	return floats
}
