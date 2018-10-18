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
	expectedMin    float64
	expectedMax    float64
	expectedCount  int
	expectedSum    float64
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

	testDuration := int64(80 * time.Hour)
	testAggregates := "min,max,count,sum,avg"
	testValues := []float64{-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6}

	tc := &TestConfig{
		desc:           fmt.Sprintf("Test with aggregates: Check [%s] with %v", testAggregates, testValues),
		testEndTime:    currentRoundedTimeNano,
		testStartTime:  currentRoundedTimeNano - testDuration,
		interval:       int64(10 * time.Minute),
		testDuration:   testDuration,
		numMetrics:     1,
		numLabels:      1,
		values:         testValues,
		expectedMin:    math.Inf(1),
		expectedMax:    math.Inf(-1),
		queryFunc:      testAggregates,
		queryStep:      int64(time.Hour),
		tsdbAggregates: testAggregates, // Create DB with all required aggregates
	}

	for _, v := range testValues {
		tc.expectedSum += v
		tc.expectedMin = math.Min(tc.expectedMin, v)
		tc.expectedMax = math.Max(tc.expectedMax, v)
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

	testDuration := int64(96 * time.Hour)
	testAggregates := "min,max,count,sum,avg"
	testValues := []float64{-8, 0, 1, 2, 3, 4, 5, 6}

	tc := &TestConfig{
		desc:           fmt.Sprintf("Test with RAW data: Check [%s] with %v", testAggregates, testValues),
		testEndTime:    currentRoundedTimeNano,
		testStartTime:  currentRoundedTimeNano - testDuration,
		interval:       int64(10 * time.Minute),
		testDuration:   testDuration,
		numMetrics:     1,
		numLabels:      1,
		values:         testValues,
		expectedMin:    math.Inf(1),
		expectedMax:    math.Inf(-1),
		queryFunc:      testAggregates,
		queryStep:      int64(time.Hour),
		tsdbAggregates: "", // create DB without aggregates (use RAW data)
	}

	for _, v := range testValues {
		tc.expectedSum += v
		tc.expectedMin = math.Min(tc.expectedMin, v)
		tc.expectedMax = math.Max(tc.expectedMax, v)
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
	// generate data and update expected result
	testConfig.expectedCount, testConfig.expectedSum = generateData(testCtx, testConfig, adapter, testConfig.logger)

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

	var actualMin float64 = math.Inf(1)
	var actualMax float64 = math.Inf(-1)
	var actualCount int
	var actualSum float64
	var actualAvgResultsCount int
	var actualAvgResultsSum float64

	for set.Next() {
		if set.Err() != nil {
			t.Fatal(set.Err())
		}

		series := set.At()
		lset := series.Labels()
		aggr := lset.Get(aggregate.AggregateLabel)
		testConfig.logger.Debug(fmt.Sprintf("\nLables: %v", lset))

		iter := series.Iterator()
		for iter.Next() {

			tm, v := iter.At()
			testConfig.logger.Debug(fmt.Sprintf("--> Aggregate: %s t=%d,v=%f\n", aggr, tm, v))

			switch aggr {
			case "min":
				actualMin = math.Min(actualMin, v)
			case "max":
				actualMax = math.Max(actualMax, v)
			case "count":
				actualCount += int(v)
			case "sum":
				actualSum += v
			case "avg":
				actualAvgResultsCount += 1
				actualAvgResultsSum = actualAvgResultsSum + v
			}
		}

		if iter.Err() != nil {
			t.Fatal(iter.Err())
		}
	}

	assert.Equal(t, testConfig.expectedMin, actualMin, "Minimal value is not as expected")

	assert.Equal(t, testConfig.expectedMax, actualMax, "Maximal value is not as expected")

	assert.Equal(t, testConfig.expectedCount, actualCount, "Count is not as expected")

	assert.Equal(t, testConfig.expectedSum, actualSum, "Sum is not as expected")

	assert.Equal(t, testConfig.expectedSum/float64(testConfig.expectedCount), actualSum/float64(actualCount),
		"Average is not as expected. [1]")

	assert.Equal(t, testConfig.expectedSum/float64(testConfig.expectedCount), actualAvgResultsSum/float64(actualAvgResultsCount),
		"Average is not as expected. [2] %f/%d != %f/%d", testConfig.expectedSum, testConfig.expectedCount, actualAvgResultsSum, actualAvgResultsCount)
}

func nanosToMillis(nanos int64) int64 {
	millis := nanos / int64(time.Millisecond)
	return millis
}

func generateData(t *testing.T, testConfig *TestConfig, adapter *tsdb.V3ioAdapter, logger logger.Logger) (totalCount int, totalSum float64) {
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

	index := 0
	var curTime int64
	for curTime = testConfig.testStartTime; curTime < testConfig.testStartTime+testConfig.testDuration; curTime += testConfig.interval {
		v := testConfig.values[index]
		err := writeNext(appender, metrics, nanosToMillis(curTime), v)
		if err != nil {
			t.Fatal(err)
		}

		totalCount++
		totalSum += v

		index = (index + 1) % len(testConfig.values)
	}
	logger.Debug(fmt.Sprintf("total samples written: %d; total sum: %f", totalCount, totalSum))

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
