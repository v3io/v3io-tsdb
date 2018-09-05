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
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"testing"
	"time"
)

type TestConfig struct {
	desc          string
	testStartTime int64
	testBaseTime  int64
	interval      int64
	testDuration  int64
	numMetrics    int
	numLabels     int
	values        []float64
	queryFunc     string
	queryStart    int64
	queryStep     int64
	expectedCount int
	expectedSum   float64
	expectedAvg   float64
	expectFail    bool
	ignoreReason  string
	v3ioConfig    *config.V3ioConfig
	setup         func() (*tsdb.V3ioAdapter, func())
}

type metricContext struct {
	lset utils.Labels
	ref  uint64
}

func TestAggregates(t *testing.T) {

	testCases := []*TestConfig{
		t1Config(t),
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
	currentTimeNano := time.Now().UnixNano()

	testDuration := int64(80 * time.Hour)

	s := &TestConfig{
		desc:          "Test case #1",
		testStartTime: currentTimeNano,
		testBaseTime:  currentTimeNano - testDuration - int64(time.Minute),
		interval:      int64(10 * time.Second),
		testDuration:  testDuration,
		numMetrics:    1,
		numLabels:     1,
		values:        []float64{1, 2, 3, 4, 5},
		queryFunc:     "count,sum,avg",
		queryStart:    currentTimeNano - int64(88*time.Hour),
		queryStep:     int64(1 * time.Hour),
		setup: func() (*tsdb.V3ioAdapter, func()) {
			return setupFunc(testCtx)
		},
	}
	return s
}

// Setup for test and return tear-down function
func setupFunc(testCtx *testing.T) (*tsdb.V3ioAdapter, func()) {
	// setup:
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		testCtx.Fatalf("Failed to read config %s", err)
	}
	v3ioConfig.Path = fmt.Sprintf("%s-%d", testCtx.Name(), time.Now().Nanosecond())

	tsdbtest.CreateTestTSDB(testCtx, v3ioConfig)

	adapter, err := tsdb.NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		testCtx.Fatal(err)
	}

	return adapter, func() {
		// Tear down:
		// Don't delete the table if the test has failed
		if !testCtx.Failed() {
			tsdbtest.DeleteTSDB(testCtx, v3ioConfig)
		}
	}
}

func testAggregatesCase(t *testing.T, testConfig *TestConfig) {
	// Setup & TearDown
	adapter, tearDown := testConfig.setup()
	defer tearDown()

	startBefore := testConfig.testStartTime - testConfig.queryStart - int64(1*time.Minute)

	total := generateData(t, testConfig, adapter)

	qry, err := adapter.Querier(nil, startBefore, testConfig.testStartTime)
	if err != nil {
		t.Fatal(err)
	}

	set, err := qry.Select("metric0", testConfig.queryFunc, nanosToMillis(testConfig.queryStep), "")
	if err != nil {
		t.Fatal(err)
	}

	expectedCount := testConfig.queryStep / testConfig.interval
	expectedSum := 0.0
	for _, v := range testConfig.values {
		expectedSum += v
	}
	expectedSum = expectedSum * float64(expectedCount) / float64(len(testConfig.values))
	totalCount := 0

	for set.Next() {
		if set.Err() != nil {
			t.Fatal(set.Err())
		}

		series := set.At()
		lset := series.Labels()
		aggr := lset.Get("Aggregator")
		fmt.Println("\n\nLables:", lset)
		iter := series.Iterator()
		for iter.Next() {

			tm, v := iter.At()
			fmt.Printf("t=%d,v=%f; ", tm, v)
			if aggr == "count" {
				totalCount += int(v)
			}

			if tm > testConfig.testBaseTime && tm < testConfig.testStartTime-testConfig.queryStep {
				if aggr == "count" && int64(v) != expectedCount {
					fmt.Println("\n***", testConfig.testStartTime, testConfig.testStartTime-3*testConfig.queryStep-int64(time.Minute))
					t.Errorf("Count is not ok - expected %d, got %f at time %d", expectedCount, v, tm)
				}
				if aggr == "sum" && v != expectedSum {
					t.Errorf("Sum is not ok - expected %f, got %f at time %d", expectedSum, v, tm)
				}
				if aggr == "avg" && v != expectedSum/float64(expectedCount) {
					t.Errorf("Avg is not ok - expected %f, got %f at time %d", expectedSum/float64(expectedCount), v, tm)
				}
			}

		}
		if iter.Err() != nil {
			t.Fatal(iter.Err())
		}

		if aggr == "count" {
			fmt.Println("\nTotal count read:", totalCount)
			if totalCount != total {
				t.Fatalf("Expected total count of %d, got %d", total, totalCount)
			}
		}

		fmt.Println()
	}
}

func nanosToMillis(nanos int64) int64 {
	millis := nanos / int64(time.Millisecond)
	return millis
}

func generateData(t *testing.T, testConfig *TestConfig, adapter *tsdb.V3ioAdapter) int {
	var metrics []*metricContext
	for m := 0; m < testConfig.numMetrics; m++ {
		for l := 0; l < testConfig.numLabels; l++ {
			metrics = append(metrics, &metricContext{
				lset: utils.FromStrings(
					"__name__", fmt.Sprintf("metric%d", m), "label", fmt.Sprintf("lbl%d", l)),
			})
		}
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatal(err)
	}

	index := 0
	total := 0
	var curTime int64
	for curTime = testConfig.testBaseTime; curTime < testConfig.testBaseTime+testConfig.testDuration; curTime += testConfig.interval {
		v := testConfig.values[index]
		err := writeNext(appender, metrics, nanosToMillis(curTime), v)
		if err != nil {
			t.Fatal(err)
		}
		index = (index + 1) % len(testConfig.values)
		total++
	}
	fmt.Println("total samples written:", total)

	appender.WaitForCompletion(10 * time.Second)

	return total
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
