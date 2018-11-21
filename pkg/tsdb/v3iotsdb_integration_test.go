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

package tsdb_test

import (
	"encoding/json"
	"math"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest/testutils"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const defaultStepMs = 5 * tsdbtest.MinuteInMillis // 5 minutes

func TestIngestData(t *testing.T) {
	testCases := []struct {
		desc   string
		params tsdbtest.TestParams
	}{
		{desc: "Should ingest one data point",
			params: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 314.3}},
					}}},
			),
		},
		{desc: "Should ingest multiple data points",
			params: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 314.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 + 10, Value: 3234.6}},
					}}},
			),
		},
		{desc: "Should ingest record with late arrival same chunk",
			params: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 314.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 - 10, Value: 3234.6}},
					}}},
			),
		},
		{desc: "Should ingest record with a dash in the metric name (IG-8585)",
			params: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cool-cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 314.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 - 10, Value: 3234.6}},
					}}},
			),
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			if test.params.IgnoreReason() != "" {
				t.Skip(test.params.IgnoreReason())
			}
			testIngestDataCase(t, test.params)
		})
	}
}

func testIngestDataCase(t *testing.T, testParams tsdbtest.TestParams) {
	defer tsdbtest.SetUp(t, testParams)()

	adapter, err := NewV3ioAdapter(testParams.V3ioConfig(), nil, nil)
	if err != nil {
		t.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatalf("Failed to get appender. reason: %s", err)
	}

	for _, dp := range testParams.TimeSeries() {
		sort.Sort(tsdbtest.DataPointTimeSorter(dp.Data))
		from := dp.Data[0].Time
		to := dp.Data[len(dp.Data)-1].Time

		labels := utils.Labels{utils.Label{Name: "__name__", Value: dp.Name}}
		labels = append(labels, dp.Labels...)

		ref, err := appender.Add(labels, dp.Data[0].Time, dp.Data[0].Value)
		if err != nil {
			t.Fatalf("Failed to add data to appender. reason: %s", err)
		}
		for _, curr := range dp.Data[1:] {
			appender.AddFast(labels, ref, curr.Time, curr.Value)
		}

		if _, err := appender.WaitForCompletion(0); err != nil {
			t.Fatalf("Failed to wait for appender completion. reason: %s", err)
		}

		tsdbtest.ValidateCountOfSamples(t, adapter, dp.Name, len(dp.Data), from, to, -1)
	}
}

func TestQueryData(t *testing.T) {
	testCases := []struct {
		desc         string
		testParams   tsdbtest.TestParams
		filter       string
		aggregates   string
		from         int64
		to           int64
		step         int64
		expected     map[string][]tsdbtest.DataPoint
		ignoreReason string
		expectFail   bool
	}{
		{desc: "Should ingest and query one data point",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("testLabel", "balbala"),
						Data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}},
					}}},
			),
			from:     0,
			to:       1532940510 + 1,
			step:     defaultStepMs,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 314.3}}}},

		{desc: "Should ingest and query multiple data points",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510 - 10, Value: 314.3},
							{Time: 1532940510 - 5, Value: 300.3},
							{Time: 1532940510, Value: 3234.6}},
					}}},
			),
			from: 0,
			to:   1532940510 + 1,
			step: defaultStepMs,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510 - 10, Value: 314.3},
				{Time: 1532940510 - 5, Value: 300.3},
				{Time: 1532940510, Value: 3234.6}}}},

		{desc: "Should query with filter on metric name",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 33.3}},
					}}},
			),
			filter:   "_name=='cpu'",
			from:     0,
			to:       1532940510 + 1,
			step:     defaultStepMs,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 33.3}}}},

		{desc: "Should query with filter on label name",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 31.3}},
					}}},
			),
			filter:   "os=='linux'",
			from:     0,
			to:       1532940510 + 1,
			step:     defaultStepMs,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 31.3}}}},

		{desc: "Should ingest and query data with '-' in the metric name (IG-8585)",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cool-cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}},
					}}},
			),
			from:     0,
			to:       1532940510 + 1,
			step:     defaultStepMs,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 314.3}}}},

		{desc: "Should ingest and query by time",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 314.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 + 10, Value: 3234.6}},
					}}},
			),
			from: 1532940510 + 2,
			to:   1532940510 + 12,
			step: defaultStepMs,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 + 10, Value: 3234.6}}}},

		{desc: "Should ingest and query by time with no results",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 314.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 + 10, Value: 3234.6}},
					}}},
			),
			from:     1532940510 + 1,
			to:       1532940510 + 4,
			step:     defaultStepMs,
			expected: map[string][]tsdbtest.DataPoint{}},

		{desc: "Should ingest and query an aggregate",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 300.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 + 10, Value: 100.4}},
					}}},
			),
			from:       1532940510,
			to:         1532940510 + 11,
			step:       defaultStepMs,
			aggregates: "sum",
			expected:   map[string][]tsdbtest.DataPoint{"sum": {{Time: 1532940510, Value: 701.0}}}},

		{desc: "Should ingest and query an aggregate with interval greater than step size",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 300.3},
							{Time: 1532940510 + 60, Value: 300.3},
							{Time: 1532940510 + 2*60, Value: 100.4},
							{Time: 1532940510 + 5*60, Value: 200.0}},
					}}},
			),
			from:       1532940510,
			to:         1532940510 + 6*60,
			step:       defaultStepMs,
			aggregates: "sum",
			expected:   map[string][]tsdbtest.DataPoint{"sum": {{Time: 1532940510, Value: 901.0}}}},

		{desc: "Should ingest and query multiple aggregates",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 300.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 + 10, Value: 100.4}},
					}}},
			),
			from:       1532940510,
			to:         1532940510 + 11,
			step:       defaultStepMs,
			aggregates: "sum,count",
			expected: map[string][]tsdbtest.DataPoint{"sum": {{Time: 1532940510, Value: 701.0}},
				"count": {{Time: 1532940510, Value: 3}}}},

		{desc: "Should fail on query with illegal time (switch from and to)",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 314.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 + 10, Value: 3234.6}},
					}}},
			),
			from:       1532940510 + 1,
			to:         0,
			step:       defaultStepMs,
			expectFail: true,
		},

		{desc: "Should query with filter on not existing metric name",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 33.3}},
					}}},
			),
			filter:   "_name=='hahaha'",
			from:     0,
			to:       1532940510 + 1,
			step:     defaultStepMs,
			expected: map[string][]tsdbtest.DataPoint{}},

		{desc: "Should ingest and query aggregates with empty bucket",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1537972278402, Value: 300.3},
							{Time: 1537972278402 + 8*tsdbtest.MinuteInMillis, Value: 300.3},
							{Time: 1537972278402 + 9*tsdbtest.MinuteInMillis, Value: 100.4}},
					}}},
			),
			from:       1537972278402 - 5*tsdbtest.MinuteInMillis,
			to:         1537972278402 + 10*tsdbtest.MinuteInMillis,
			step:       defaultStepMs,
			aggregates: "count",
			expected: map[string][]tsdbtest.DataPoint{
				"count": {{Time: 1537972278402, Value: 1},
					{Time: 1537972578402, Value: 2}}}},

		{desc: "Should ingest and query aggregates with few empty buckets in a row",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1537972278402, Value: 300.3},
							{Time: 1537972278402 + 16*tsdbtest.MinuteInMillis, Value: 300.3},
							{Time: 1537972278402 + 17*tsdbtest.MinuteInMillis, Value: 100.4}},
					}}},
			),
			from:       1537972278402 - 5*tsdbtest.MinuteInMillis,
			to:         1537972278402 + 18*tsdbtest.MinuteInMillis,
			step:       defaultStepMs,
			aggregates: "count",
			expected: map[string][]tsdbtest.DataPoint{
				"count": {{Time: 1537972158402, Value: 1},
					{Time: 1537973058402, Value: 2}}}},

		{desc: "Should ingest and query server-side aggregates",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 300.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 + 10, Value: 100.4}},
					}}},
			),
			from:       1532940510,
			to:         1532940510 + 11,
			step:       60 * tsdbtest.MinuteInMillis,
			aggregates: "sum,count,min,max,sqr,last",
			expected: map[string][]tsdbtest.DataPoint{"sum": {{Time: 1532940510, Value: 701.0}},
				"count": {{Time: 1532940510, Value: 3}},
				"min":   {{Time: 1532940510, Value: 100.4}},
				"max":   {{Time: 1532940510, Value: 300.3}},
				"sqr":   {{Time: 1532940510, Value: 190440.3}},
				"last":  {{Time: 1532940510, Value: 100.4}}}},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testQueryDataCase(t, test.testParams, test.filter, test.aggregates, test.from, test.to, test.step, test.expected, test.expectFail)
		})
	}
}

func testQueryDataCase(test *testing.T, testParams tsdbtest.TestParams, filter string, queryAggregates string,
	from int64, to int64, step int64, expected map[string][]tsdbtest.DataPoint, expectFail bool) {

	adapter, teardown := tsdbtest.SetUpWithData(test, testParams)
	defer teardown()

	qry, err := adapter.Querier(nil, from, to)
	if err != nil {
		if expectFail {
			return
		} else {
			test.Fatalf("Failed to create Querier. reason: %v", err)
		}
	}

	for _, metric := range testParams.TimeSeries() {
		set, err := qry.Select(metric.Name, queryAggregates, step, filter)
		if err != nil {
			test.Fatalf("Failed to run Select. reason: %v", err)
		}

		var counter int
		for counter = 0; set.Next(); counter++ {
			if set.Err() != nil {
				test.Fatalf("Failed to query metric. reason: %v", set.Err())
			}

			series := set.At()
			currentAggregate := series.Labels().Get(aggregate.AggregateLabel)
			iter := series.Iterator()
			if iter.Err() != nil {
				test.Fatalf("Failed to query data series. reason: %v", iter.Err())
			}

			actual, err := iteratorToSlice(iter)
			if err != nil {
				test.Fatal(err)
			}
			assert.ElementsMatch(test, expected[currentAggregate], actual,
				"Check failed for aggregate='%s'. Query aggregates: %s", currentAggregate, queryAggregates)
		}

		if set.Err() != nil {
			test.Fatalf("Failed to query metric. reason: %v", set.Err())
		}
		if counter == 0 && len(expected) > 0 {
			test.Fatalf("No data was received")
		}
	}
}

func TestQueryDataOverlappingWindow(t *testing.T) {
	v3ioConfig, err := config.GetOrDefaultConfig()
	if err != nil {
		t.Fatalf("unable to load configuration. Error: %v", err)
	}

	testCases := []struct {
		desc         string
		metricName   string
		labels       []utils.Label
		data         []tsdbtest.DataPoint
		filter       string
		aggregates   string
		windows      []int
		from         int64
		to           int64
		expected     map[string][]tsdbtest.DataPoint
		ignoreReason string
	}{
		{desc: "Should ingest and query with windowing",
			metricName: "cpu",
			labels:     utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532944110, Value: 314.3},
				{Time: 1532947710, Value: 300.3},
				{Time: 1532951310, Value: 3234.6}},
			from: 0, to: 1532954910,
			windows:    []int{1, 2, 4},
			aggregates: "sum",
			expected: map[string][]tsdbtest.DataPoint{
				"sum": {{Time: 1532937600, Value: 4163.5},
					{Time: 1532944800, Value: 3534.9},
					{Time: 1532948400, Value: 3234.6}}},
		},

		{desc: "Should ingest and query with windowing on multiple agg",
			metricName: "cpu",
			labels:     utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532944110, Value: 314.3},
				{Time: 1532947710, Value: 300.3},
				{Time: 1532951310, Value: 3234.6}},
			from: 0, to: 1532954910,
			windows:    []int{1, 2, 4},
			aggregates: "sum,count,sqr",
			expected: map[string][]tsdbtest.DataPoint{
				"sum": {{Time: 1532937600, Value: 4163.5},
					{Time: 1532944800, Value: 3534.9},
					{Time: 1532948400, Value: 3234.6}},
				"count": {{Time: 1532937600, Value: 4},
					{Time: 1532944800, Value: 2},
					{Time: 1532948400, Value: 1}},
				"sqr": {{Time: 1532937600, Value: 10750386.23},
					{Time: 1532944800, Value: 10552817.25},
					{Time: 1532948400, Value: 10462637.16}},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testQueryDataOverlappingWindowCase(t, v3ioConfig, test.metricName, test.labels,
				test.data, test.filter, test.windows, test.aggregates, test.from, test.to, test.expected)
		})
	}
}

func testQueryDataOverlappingWindowCase(test *testing.T, v3ioConfig *config.V3ioConfig,
	metricsName string, userLabels []utils.Label, data []tsdbtest.DataPoint, filter string,
	windows []int, agg string,
	from int64, to int64, expected map[string][]tsdbtest.DataPoint) {

	testParams := tsdbtest.NewTestParams(test,
		tsdbtest.TestOption{Key: tsdbtest.OptV3ioConfig, Value: v3ioConfig},
		tsdbtest.TestOption{Key: tsdbtest.OptTimeSeries, Value: tsdbtest.TimeSeries{tsdbtest.Metric{Name: metricsName, Data: data, Labels: userLabels}}},
	)

	adapter, teardown := tsdbtest.SetUpWithData(test, testParams)
	defer teardown()

	var step int64 = 3600

	qry, err := adapter.Querier(nil, from, to)
	if err != nil {
		test.Fatalf("Failed to create Querier. reason: %v", err)
	}

	set, err := qry.SelectOverlap(metricsName, agg, step, windows, filter)
	if err != nil {
		test.Fatalf("Failed to run Select. reason: %v", err)
	}

	var counter int
	for counter = 0; set.Next(); counter++ {
		if set.Err() != nil {
			test.Fatalf("Failed to query metric. reason: %v", set.Err())
		}

		series := set.At()
		agg := series.Labels().Get(aggregate.AggregateLabel)
		iter := series.Iterator()
		if iter.Err() != nil {
			test.Fatalf("Failed to query data series. reason: %v", iter.Err())
		}

		actual, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			test.Fatal(err)
		}
		assert.EqualValues(test, len(windows), len(actual))
		for _, data := range expected[agg] {
			assert.Contains(test, actual, data)
		}
	}

	if set.Err() != nil {
		test.Fatalf("Failed to query metric. reason: %v", set.Err())
	}
	if counter == 0 && len(expected) > 0 {
		test.Fatalf("No data was received")
	}
}

// Calling Seek instead of next for the first time while iterating over data (TSDB-43)
func TestIgnoreNaNWhenSeekingAggSeries(t *testing.T) {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		t.Fatalf("unable to load configuration. Error: %v", err)
	}
	metricsName := "cpu"
	baseTime := int64(1532940510000)
	userLabels := utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease")
	data := []tsdbtest.DataPoint{{Time: baseTime, Value: 300.3},
		{Time: baseTime + tsdbtest.MinuteInMillis, Value: 300.3},
		{Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 100.4},
		{Time: baseTime + 5*tsdbtest.MinuteInMillis, Value: 200.0}}
	from := int64(baseTime - 60*tsdbtest.MinuteInMillis)
	to := int64(baseTime + 6*tsdbtest.MinuteInMillis)
	step := int64(2 * tsdbtest.MinuteInMillis)
	agg := "avg"
	expected := map[string][]tsdbtest.DataPoint{
		"avg": {{baseTime, 300.3},
			{baseTime + step, 100.4},
			{baseTime + 2*step, 200}}}

	testParams := tsdbtest.NewTestParams(t,
		tsdbtest.TestOption{Key: tsdbtest.OptV3ioConfig, Value: v3ioConfig},
		tsdbtest.TestOption{Key: tsdbtest.OptTimeSeries, Value: tsdbtest.TimeSeries{tsdbtest.Metric{Name: metricsName, Data: data, Labels: userLabels}}},
	)

	adapter, teardown := tsdbtest.SetUpWithData(t, testParams)
	defer teardown()

	qry, err := adapter.Querier(nil, from, to)
	if err != nil {
		t.Fatalf("Failed to create Querier. reason: %v", err)
	}

	set, err := qry.Select(metricsName, agg, step, "")
	if err != nil {
		t.Fatalf("Failed to run Select. reason: %v", err)
	}

	var counter int
	for counter = 0; set.Next(); counter++ {
		if set.Err() != nil {
			t.Fatalf("Failed to query metric. reason: %v", set.Err())
		}

		series := set.At()
		agg := series.Labels().Get(aggregate.AggregateLabel)
		iter := series.Iterator()
		if iter.Err() != nil {
			t.Fatalf("Failed to query data series. reason: %v", iter.Err())
		}
		if !iter.Seek(0) {
			t.Fatal("Seek time returned false, iterator error:", iter.Err())
		}
		var actual []tsdbtest.DataPoint
		t0, v0 := iter.At()
		if iter.Err() != nil {
			t.Fatal("error iterating over series", iter.Err())
		}
		actual = append(actual, tsdbtest.DataPoint{Time: t0, Value: v0})
		for iter.Next() {
			t1, v1 := iter.At()

			if iter.Err() != nil {
				t.Fatal("error iterating over series", iter.Err())
			}
			actual = append(actual, tsdbtest.DataPoint{Time: t1, Value: v1})
		}
		assert.ElementsMatch(t, expected[agg], actual)
	}

	if set.Err() != nil {
		t.Fatalf("Failed to query metric. reason: %v", set.Err())
	}
	if counter == 0 && len(expected) > 0 {
		t.Fatalf("No data was received")
	}
}

func TestCreateTSDB(t *testing.T) {
	testCases := []struct {
		desc         string
		conf         *config.Schema
		ignoreReason string
	}{
		{desc: "Should create TSDB with standard configuration", conf: testutils.CreateSchema(t, "sum,count")},

		{desc: "Should create TSDB with wildcard aggregations", conf: testutils.CreateSchema(t, "*")},
	}

	testParams := tsdbtest.NewTestParams(t)

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testCreateTSDBcase(t, test.conf, testParams)
		})
	}
}

func testCreateTSDBcase(t *testing.T, dbConfig *config.Schema, testParams tsdbtest.TestParams) {
	defer tsdbtest.SetUpWithDBConfig(t, dbConfig, testParams)()

	adapter, err := NewV3ioAdapter(testParams.V3ioConfig(), nil, nil)
	if err != nil {
		t.Fatalf("Failed to create adapter. reason: %s", err)
	}

	actualDbConfig := adapter.GetSchema()
	assert.Equal(t, actualDbConfig, dbConfig)
}

func TestDeleteTSDB(t *testing.T) {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		t.Fatalf("unable to load configuration. Error: %v", err)
	}

	schema := testutils.CreateSchema(t, "count,sum")
	v3ioConfig.TablePath = tsdbtest.PrefixTablePath(t.Name())
	if err := CreateTSDB(v3ioConfig, schema); err != nil {
		v3ioConfigAsJson, _ := json.MarshalIndent(v3ioConfig, "", "  ")
		t.Fatalf("Failed to create TSDB. Reason: %s\nConfiguration:\n%s", err, string(v3ioConfigAsJson))
	}

	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}
	responseChan := make(chan *v3io.Response)
	container, _ := adapter.GetContainer()
	container.ListBucket(&v3io.ListBucketInput{Path: v3ioConfig.TablePath}, 30, responseChan)
	if res := <-responseChan; res.Error != nil {
		t.Fatal("Failed to create TSDB")
	}

	now := time.Now().Unix() * 1000 // now time in millis
	if err := adapter.DeleteDB(true, true, 0, now); err != nil {
		t.Fatalf("Failed to delete DB on teardown. reason: %s", err)
	}

	container.ListBucket(&v3io.ListBucketInput{Path: v3ioConfig.TablePath}, 30, responseChan)
	if res := <-responseChan; res.Error == nil {
		t.Fatal("Did not delete TSDB properly")
	}
}

func TestDeleteTable(t *testing.T) {
	ta, _ := time.Parse(time.RFC3339, "2018-10-03T05:00:00Z")
	t1 := ta.Unix() * 1000
	tb, _ := time.Parse(time.RFC3339, "2018-10-07T05:00:00Z")
	t2 := tb.Unix() * 1000
	tc, _ := time.Parse(time.RFC3339, "2018-10-11T05:00:00Z")
	t3 := tc.Unix() * 1000
	td, _ := time.Parse(time.RFC3339, "now + 1w")
	futurePoint := td.Unix() * 1000

	testCases := []struct {
		desc         string
		deleteFrom   int64
		deleteTo     int64
		deleteAll    bool
		ignoreErrors bool
		data         []tsdbtest.DataPoint
		expected     []tsdbtest.DataPoint
		ignoreReason string
	}{
		{desc: "Should delete all table by time",
			deleteFrom:   0,
			deleteTo:     9999999999999,
			deleteAll:    false,
			ignoreErrors: true,
			data: []tsdbtest.DataPoint{{Time: t1, Value: 222.2},
				{Time: t2, Value: 333.3},
				{Time: t3, Value: 444.4}},
			expected: []tsdbtest.DataPoint{},
		},
		{desc: "Should delete all table by deleteAll",
			deleteFrom:   0,
			deleteTo:     0,
			deleteAll:    true,
			ignoreErrors: true,
			data: []tsdbtest.DataPoint{{Time: t1, Value: 222.2},
				{Time: t2, Value: 333.3},
				{Time: t3, Value: 444.4},
				{Time: futurePoint, Value: 555.5}},
			expected: []tsdbtest.DataPoint{},
		},
		{desc: "Should skip partial partition at begining",
			deleteFrom:   t1 - 10000,
			deleteTo:     9999999999999,
			deleteAll:    false,
			ignoreErrors: true,
			data: []tsdbtest.DataPoint{{Time: t1, Value: 222.2},
				{Time: t2, Value: 333.3},
				{Time: t3, Value: 444.4}},
			expected: []tsdbtest.DataPoint{{Time: t1, Value: 222.2}},
		},
		{desc: "Should skip partial partition at end",
			deleteFrom:   0,
			deleteTo:     t3 + 10000,
			deleteAll:    false,
			ignoreErrors: true,
			data: []tsdbtest.DataPoint{{Time: t1, Value: 222.2},
				{Time: t2, Value: 333.3},
				{Time: t3, Value: 444.4}},
			expected: []tsdbtest.DataPoint{{Time: t3, Value: 444.4}},
		},
		{desc: "Should skip partial partition at beginning and end not in range",
			deleteFrom:   t1 + 10000,
			deleteTo:     t3 - 10000,
			deleteAll:    false,
			ignoreErrors: true,
			data: []tsdbtest.DataPoint{{Time: t1, Value: 222.2},
				{Time: t2, Value: 333.3},
				{Time: t3, Value: 444.4}},
			expected: []tsdbtest.DataPoint{{Time: t1, Value: 222.2},
				{Time: t3, Value: 444.4}},
		},
		{desc: "Should skip partial partition at beginning and end although in range",
			deleteFrom:   t1 - 10000,
			deleteTo:     t3 + 10000,
			deleteAll:    false,
			ignoreErrors: true,
			data: []tsdbtest.DataPoint{{Time: t1, Value: 222.2},
				{Time: t2, Value: 333.3},
				{Time: t3, Value: 444.4}},
			expected: []tsdbtest.DataPoint{{Time: t1, Value: 222.2},
				{Time: t3, Value: 444.4}},
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testDeleteTSDBCase(t,
				tsdbtest.NewTestParams(t,
					tsdbtest.TestOption{
						Key:   tsdbtest.OptDropTableOnTearDown,
						Value: !test.deleteAll},
					tsdbtest.TestOption{
						Key: tsdbtest.OptTimeSeries,
						Value: tsdbtest.TimeSeries{tsdbtest.Metric{
							Name:   "metricToDelete",
							Labels: utils.LabelsFromStringList("os", "linux"),
							Data:   test.data,
						}}},
				),
				test.deleteFrom, test.deleteTo, test.ignoreErrors, test.deleteAll, test.expected)
		})
	}
}

func testDeleteTSDBCase(test *testing.T, testParams tsdbtest.TestParams, deleteFrom int64, deleteTo int64, ignoreErrors bool, deleteAll bool,
	expected []tsdbtest.DataPoint) {

	adapter, teardown := tsdbtest.SetUpWithData(test, testParams)
	defer teardown()

	pm, err := partmgr.NewPartitionMngr(adapter.GetSchema(), nil, testParams.V3ioConfig())
	if err != nil {
		test.Fatalf("Failed to create new partition manager. reason: %s", err)
	}

	initiaPartitions := pm.PartsForRange(0, math.MaxInt64, true)
	initialNumberOfPartitions := len(initiaPartitions)

	partitionsToDelete := pm.PartsForRange(deleteFrom, deleteTo, false)

	if err := adapter.DeleteDB(deleteAll, ignoreErrors, deleteFrom, deleteTo); err != nil {
		test.Fatalf("Failed to delete DB. reason: %s", err)
	}

	if !deleteAll {
		pm1, err := partmgr.NewPartitionMngr(adapter.GetSchema(), nil, testParams.V3ioConfig())
		remainingParts := pm1.PartsForRange(0, math.MaxInt64, false)
		assert.Equal(test, len(remainingParts), initialNumberOfPartitions-len(partitionsToDelete))

		qry, err := adapter.Querier(nil, 0, math.MaxInt64)
		if err != nil {
			test.Fatalf("Failed to create Querier. reason: %v", err)
		}

		for _, metric := range testParams.TimeSeries() {
			set, err := qry.Select(metric.Name, "", 0, "")
			if err != nil {
				test.Fatalf("Failed to run Select. reason: %v", err)
			}

			set.Next()
			if set.Err() != nil {
				test.Fatalf("Failed to query metric. reason: %v", set.Err())
			}

			series := set.At()
			if series == nil && len(expected) == 0 {
				//table is expected to be empty
			} else if series != nil {
				iter := series.Iterator()
				if iter.Err() != nil {
					test.Fatalf("Failed to query data series. reason: %v", iter.Err())
				}

				actual, err := iteratorToSlice(iter)
				if err != nil {
					test.Fatal(err)
				}
				assert.ElementsMatch(test, expected, actual)
			} else {
				test.Fatalf("Result series is empty while expected result set is not!")
			}
		}
	} else {
		container, tablePath := adapter.GetContainer()
		tableSchemaPath := path.Join(tablePath, config.SchemaConfigFileName)

		// Validate: schema does not exist
		_, err := container.Sync.GetObject(&v3io.GetObjectInput{Path: tableSchemaPath})
		if err != nil {
			if utils.IsNotExistsError(err) {
				// OK - expected
			} else {
				test.Fatalf("Failed to read a TSDB schema from '%s'.\nError: %v", tableSchemaPath, err)
			}
		}

		// Validate: table does not exist
		_, err = container.Sync.GetObject(&v3io.GetObjectInput{Path: tablePath})
		if err != nil {
			if utils.IsNotExistsError(err) {
				// OK - expected
			} else {
				test.Fatalf("Failed to read a TSDB schema from '%s'.\nError: %v", tablePath, err)
			}
		}
	}
}

func iteratorToSlice(it chunkenc.Iterator) ([]tsdbtest.DataPoint, error) {
	var result []tsdbtest.DataPoint
	for it.Next() {
		t, v := it.At()
		if it.Err() != nil {
			return nil, it.Err()
		}
		result = append(result, tsdbtest.DataPoint{Time: t, Value: v})
	}
	return result, nil
}
