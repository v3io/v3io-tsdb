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
	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest/testutils"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"path/filepath"
	"sort"
	"testing"
)

func TestIngestData(t *testing.T) {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		t.Fatalf("Failed to load test configuration. reason: %s", err)
	}

	testCases := []struct {
		desc         string
		metricName   string
		labels       []utils.Label
		data         []tsdbtest.DataPoint
		ignoreReason string
	}{
		{desc: "Should ingest one data point", metricName: "cpu", labels: utils.FromStrings("testLabel", "balbala"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}}},

		{desc: "Should ingest multiple data points", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 + 10, Value: 3234.6}}},

		{desc: "Should ingest record with late arrival same chunk", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 - 10, Value: 3234.6}}},

		{desc: "Should ingest record with a dash in the metric name (IG-8585)", metricName: "cool-cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 - 10, Value: 3234.6}}},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testIngestDataCase(t, v3ioConfig, test.metricName, test.labels, test.data)
		})
	}
}

func testIngestDataCase(t *testing.T, v3ioConfig *config.V3ioConfig,
	metricsName string, userLabels []utils.Label, data []tsdbtest.DataPoint) {
	defer tsdbtest.SetUp(t, v3ioConfig)()

	sort.Sort(tsdbtest.DataPointTimeSorter(data))
	from := data[0].Time
	to := data[len(data)-1].Time

	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatalf("Failed to get appender. reason: %s", err)
	}

	labels := utils.Labels{utils.Label{Name: "__name__", Value: metricsName}}
	labels = append(labels, userLabels...)

	ref, err := appender.Add(labels, data[0].Time, data[0].Value)
	if err != nil {
		t.Fatalf("Failed to add data to appender. reason: %s", err)
	}
	for _, curr := range data[1:] {
		appender.AddFast(labels, ref, curr.Time, curr.Value)
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		t.Fatalf("Failed to wait for appender completion. reason: %s", err)
	}

	tsdbtest.ValidateCountOfSamples(t, adapter, metricsName, len(data), from, to)
}

func TestQueryData(t *testing.T) {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		t.Fatalf("Failed to load test configuration. reason: %s", err)
	}

	testCases := []struct {
		desc         string
		metricName   string
		labels       []utils.Label
		data         []tsdbtest.DataPoint
		filter       string
		aggregators  string
		from         int64
		to           int64
		expected     map[string][]tsdbtest.DataPoint
		ignoreReason string
		expectFail   bool
	}{
		{desc: "Should ingest and query one data point", metricName: "cpu",
			labels: utils.FromStrings("testLabel", "balbala"),
			data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}},
			from:   0, to: 1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 314.3}}}},

		{desc: "Should ingest and query multiple data points", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510 - 10, Value: 314.3},
				{Time: 1532940510 - 5, Value: 300.3},
				{Time: 1532940510, Value: 3234.6}},
			from: 0, to: 1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510 - 10, Value: 314.3},
				{Time: 1532940510 - 5, Value: 300.3},
				{Time: 1532940510, Value: 3234.6}}}},

		{desc: "Should query with filter on metric name", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 33.3}},
			filter: "_name=='cpu'",
			from:   0, to: 1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 33.3}}}},

		{desc: "Should query with filter on label name", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 31.3}},
			filter: "os=='linux'",
			from:   0, to: 1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 31.3}}}},

		{desc: "Should ingest and query data with '-' in the metric name (IG-8585)", metricName: "cool-cpu",
			labels: utils.FromStrings("testLabel", "balbala"),
			data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}},
			from:   0, to: 1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 314.3}}}},

		{desc: "Should ingest and query by time", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 + 10, Value: 3234.6}},
			from: 1532940510 + 2, to: 1532940510 + 12,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 + 10, Value: 3234.6}}}},

		{desc: "Should ingest and query by time with no results", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 + 10, Value: 3234.6}},
			from: 1532940510 + 1, to: 1532940510 + 4,
			expected: map[string][]tsdbtest.DataPoint{}},

		{desc: "Should ingest and query an aggregator", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 300.3},
				{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 + 10, Value: 100.4}},
			from: 1532940510, to: 1532940510 + 11,
			aggregators: "sum",
			expected:    map[string][]tsdbtest.DataPoint{"sum": {{Time: 1532940510, Value: 701.0}}}},

		{desc: "Should ingest and query an aggregator EXTRA", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 300.3},
				{Time: 1532940510 + 60, Value: 300.3},
				{Time: 1532940510 + 2*60, Value: 100.4},
				{Time: 1532940510 + 2*60, Value: 200.0}},
			from:        1532940510,
			to:          1532940510 + 6*60,
			aggregators: "sum",
			expected:    map[string][]tsdbtest.DataPoint{"sum": {{Time: 1532940510, Value: 901.0}}}},

		{desc: "Should ingest and query multiple aggregators", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 300.3},
				{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 + 10, Value: 100.4}},
			from: 1532940510, to: 1532940510 + 11,
			aggregators: "sum,count",
			expected: map[string][]tsdbtest.DataPoint{"sum": {{Time: 1532940510, Value: 701.0}},
				"count": {{Time: 1532940510, Value: 3}}}},

		{desc: "Should fail on query with illegal time (switch from and to)", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 + 10, Value: 3234.6}},
			from: 1532940510 + 1, to: 0,
			expectFail: true,
		},

		{desc: "Should query with filter on not existing metric name", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 33.3}},
			filter: "_name=='hahaha'",
			from:   0, to: 1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{}},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testQueryDataCase(t, v3ioConfig, test.metricName, test.labels,
				test.data, test.filter, test.aggregators, test.from, test.to, test.expected, test.expectFail)
		})
	}
}

func testQueryDataCase(test *testing.T, v3ioConfig *config.V3ioConfig,
	metricsName string, userLabels []utils.Label, data []tsdbtest.DataPoint, filter string, aggregator string,
	from int64, to int64, expected map[string][]tsdbtest.DataPoint, expectFail bool) {
	adapter, teardown := tsdbtest.SetUpWithData(test, v3ioConfig, metricsName, data, userLabels)
	defer teardown()

	qry, err := adapter.Querier(nil, from, to)
	if err != nil {
		if expectFail {
			return
		} else {
			test.Fatalf("Failed to create Querier. reason: %v", err)
		}
	}

	step := int64(5 * 60 * 1000) // 5 minutes
	set, err := qry.Select(metricsName, aggregator, step, filter)
	if err != nil {
		test.Fatalf("Failed to run Select. reason: %v", err)
	}

	var counter int
	for counter = 0; set.Next(); counter++ {
		if set.Err() != nil {
			test.Fatalf("Failed to query metric. reason: %v", set.Err())
		}

		series := set.At()
		agg := series.Labels().Get("Aggregator")
		iter := series.Iterator()
		if iter.Err() != nil {
			test.Fatalf("Failed to query data series. reason: %v", iter.Err())
		}

		actual, err := iteratorToSlice(iter)
		if err != nil {
			test.Fatal(err)
		}
		assert.ElementsMatch(test, expected[agg], actual)
	}

	if set.Err() != nil {
		test.Fatalf("Failed to query metric. reason: %v", set.Err())
	}
	if counter == 0 && len(expected) > 0 {
		test.Fatalf("No data was received")
	}
}

func TestQueryDataOverlappingWindow(t *testing.T) {
	v3ioConfig, err := config.LoadConfig(filepath.Join("..", "..", config.DefaultConfigurationFileName))
	if err != nil {
		t.Fatalf("Failed to load test configuration. reason: %s", err)
	}

	testCases := []struct {
		desc         string
		metricName   string
		labels       []utils.Label
		data         []tsdbtest.DataPoint
		filter       string
		aggregators  string
		windows      []int
		from         int64
		to           int64
		expected     map[string][]tsdbtest.DataPoint
		ignoreReason string
	}{
		{desc: "Should ingest and query with windowing",
			metricName: "cpu",
			labels:     utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532944110, Value: 314.3},
				{Time: 1532947710, Value: 300.3},
				{Time: 1532951310, Value: 3234.6}},
			from: 0, to: 1532954910,
			windows:     []int{1, 2, 4},
			aggregators: "sum",
			expected: map[string][]tsdbtest.DataPoint{
				"sum": {{Time: 1532937600, Value: 4163.5},
					{Time: 1532944800, Value: 3534.9},
					{Time: 1532948400, Value: 3234.6}}},
		},

		{desc: "Should ingest and query with windowing on multiple agg",
			metricName: "cpu",
			labels:     utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532944110, Value: 314.3},
				{Time: 1532947710, Value: 300.3},
				{Time: 1532951310, Value: 3234.6}},
			from: 0, to: 1532954910,
			windows:     []int{1, 2, 4},
			aggregators: "sum,count,sqr",
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
				test.data, test.filter, test.windows, test.aggregators, test.from, test.to, test.expected)
		})
	}
}

func testQueryDataOverlappingWindowCase(test *testing.T, v3ioConfig *config.V3ioConfig,
	metricsName string, userLabels []utils.Label, data []tsdbtest.DataPoint, filter string,
	windows []int, aggregator string,
	from int64, to int64, expected map[string][]tsdbtest.DataPoint) {
	adapter, teardown := tsdbtest.SetUpWithData(test, v3ioConfig, metricsName, data, userLabels)
	defer teardown()

	var step int64 = 3600

	qry, err := adapter.Querier(nil, from, to)
	if err != nil {
		test.Fatalf("Failed to create Querier. reason: %v", err)
	}

	set, err := qry.SelectOverlap(metricsName, aggregator, step, windows, filter)
	if err != nil {
		test.Fatalf("Failed to run Select. reason: %v", err)
	}

	var counter int
	for counter = 0; set.Next(); counter++ {
		if set.Err() != nil {
			test.Fatalf("Failed to query metric. reason: %v", set.Err())
		}

		series := set.At()
		agg := series.Labels().Get("Aggregator")
		iter := series.Iterator()
		if iter.Err() != nil {
			test.Fatalf("Failed to query data series. reason: %v", iter.Err())
		}

		actual, err := iteratorToSlice(iter)
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

func TestCreateTSDB(t *testing.T) {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		t.Fatalf("Failed to load test configuration. reason: %s", err)
	}

	testCases := []struct {
		desc         string
		conf         config.Schema
		ignoreReason string
	}{
		{desc: "Should create TSDB with standard configuration", conf: testutils.CreateSchema(t, "sum,count")},

		{desc: "Should create TSDB with wildcard aggregations", conf: testutils.CreateSchema(t, "*")},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testCreateTSDBcase(t, v3ioConfig, test.conf)
		})
	}

}

func testCreateTSDBcase(t *testing.T, v3ioConfig *config.V3ioConfig, dbConfig config.Schema) {
	defer tsdbtest.SetUpWithDBConfig(t, v3ioConfig, &dbConfig)()

	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create adapter. reason: %s", err)
	}

	actualDbConfig := *adapter.GetSchema()
	assert.Equal(t, actualDbConfig, dbConfig)
}

func TestDeleteTSDB(t *testing.T) {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		t.Fatalf("Failed to load test configuration. reason: %s", err)
	}

	schema := testutils.CreateSchema(t, "count,sum")
	v3ioConfig.Path = t.Name()
	if err := CreateTSDB(v3ioConfig, &schema); err != nil {
		v3ioConfigAsJson, _ := json.MarshalIndent(v3ioConfig, "", "  ")
		t.Fatalf("Failed to create TSDB. Reason: %s\nConfiguration:\n%s", err, string(v3ioConfigAsJson))
	}

	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}
	responseChan := make(chan *v3io.Response)
	container, _ := adapter.GetContainer()
	container.ListBucket(&v3io.ListBucketInput{Path: v3ioConfig.Path}, 30, responseChan)
	if res := <-responseChan; res.Error != nil {
		t.Fatal("Failed to create TSDB")
	}

	if err := adapter.DeleteDB(true, true, 0, 0); err != nil {
		t.Fatalf("Failed to delete DB on teardown. reason: %s", err)
	}

	container.ListBucket(&v3io.ListBucketInput{Path: v3ioConfig.Path}, 30, responseChan)
	if res := <-responseChan; res.Error == nil {
		t.Fatal("Did not delete TSDB properly")
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
