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
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"testing"
	"time"
)

var basetime int64

type TestConfig struct {
	Interval     int64
	TimeVariance int
	TestDuration string
	NumMetrics   int
	NumLabels    int
	Values       []float64
	QueryFunc    string
	QueryStart   string
	QueryStepSec int64
}

type metricContext struct {
	lset utils.Labels
	ref  uint64
}

func TestAggregates(t *testing.T) {

	testConfig := TestConfig{
		Interval:     10,
		TimeVariance: 0,
		TestDuration: "80h",
		NumMetrics:   1,
		NumLabels:    1,
		Values:       []float64{1, 2, 3, 4, 5},
		QueryFunc:    "count,sum,avg",
		QueryStart:   "88h",
		QueryStepSec: 60 * 60,
	}

	duration, err := utils.Str2duration(testConfig.TestDuration)
	if err != nil {
		t.Fatalf("Failed to read test duration %s - %s", testConfig.TestDuration, err)
	}

	startBefore, err := utils.Str2duration(testConfig.QueryStart)
	if err != nil {
		t.Fatalf("Failed to read test query start %s - %s", testConfig.QueryStart, err)
	}

	now := time.Now().Unix() * 1000
	basetime = now - duration - 60000 // now - duration - 1 min
	startBefore = now - startBefore - 60000

	cfg, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		t.Fatalf("Failed to read config %s", err)
	}

	adapter, err := tsdb.NewV3ioAdapter(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatal(err)
	}

	metrics := []*metricContext{}
	for m := 0; m < testConfig.NumMetrics; m++ {
		for l := 0; l < testConfig.NumLabels; l++ {
			metrics = append(metrics, &metricContext{
				lset: utils.FromStrings(
					"__name__", fmt.Sprintf("metric%d", m), "label", fmt.Sprintf("lbl%d", l)),
			})
		}
	}

	index := 0
	total := 0
	var curTime int64
	for curTime = basetime; curTime < basetime+duration; curTime += testConfig.Interval * 1000 {
		v := testConfig.Values[index]
		//trand := curTime + int64(rand.Intn(testConfig.TimeVariance) - testConfig.TimeVariance/2)
		err := writeNext(appender, metrics, curTime, v)
		if err != nil {
			t.Fatal(err)
		}
		index = (index + 1) % len(testConfig.Values)
		total++
	}
	fmt.Println("total samples written:", total)

	appender.WaitForCompletion(0)
	time.Sleep(time.Second * 2)

	qry, err := adapter.Querier(nil, startBefore, now)
	if err != nil {
		t.Fatal(err)
	}

	set, err := qry.Select("metric0", testConfig.QueryFunc, testConfig.QueryStepSec*1000, "")
	if err != nil {
		t.Fatal(err)
	}

	expectedCount := testConfig.QueryStepSec / testConfig.Interval
	expectedSum := 0.0
	for _, v := range testConfig.Values {
		expectedSum += v
	}
	expectedSum = expectedSum * float64(expectedCount) / float64(len(testConfig.Values))
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

			if tm > basetime && tm < curTime-1*testConfig.QueryStepSec*1000 {
				if aggr == "count" && int64(v) != expectedCount {
					fmt.Println("\n***", curTime, curTime-3*testConfig.QueryStepSec*1000-60000)
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
