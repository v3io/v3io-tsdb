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
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/schema"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest/testutils"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const defaultStepMs = 5 * tsdbtest.MinuteInMillis // 5 minutes

func TestIngestData(t *testing.T) {
	timestamp := fmt.Sprintf("%d", time.Now().Unix()) //time.Now().Format(time.RFC3339)
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
		{desc: "Should ingest into first partition in epoch without corruption (TSDB-67)",
			params: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "coolcpu",
						Labels: utils.LabelsFromStringList("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 10, Value: 314.3},
						},
					}}},
			),
		},
		{desc: "Should drop values of incompatible data types ",
			params: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "IG13146",
						Labels: utils.LabelsFromStringList("test", "IG-13146", "float", "string"),
						Data: []tsdbtest.DataPoint{
							{Time: 15, Value: 0.1},                 // first add float value
							{Time: 20, Value: "some string value"}, // then attempt to add string value
							{Time: 30, Value: 0.2},                 // and finally add another float value
						},
						ExpectedCount: func() *int { var expectedCount = 2; return &expectedCount }(),
					}}},
				tsdbtest.TestOption{
					Key:   "override_test_name",
					Value: fmt.Sprintf("IG-13146-%s", timestamp)}),
		},
		{desc: "IG-13146: Should reject values of incompatible data types without data corruption",
			params: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "IG13146",
						Labels: utils.LabelsFromStringList("test", "IG-13146", "float", "string"),
						Data: []tsdbtest.DataPoint{
							{Time: 50, Value: "another string value"}, // then attempt to add string value
							{Time: 60, Value: 0.4},                    // valid values from this batch will be dropped
							{Time: 70, Value: 0.3},                    // because processing of entire batch will stop
						},
						ExpectedCount: func() *int { var expectedCount = 1; return &expectedCount }(),
					}}},
				tsdbtest.TestOption{
					Key:   "override_test_name",
					Value: fmt.Sprintf("IG-13146-%s", timestamp)},
				tsdbtest.TestOption{
					Key: "expected_error_contains_string",
					// Note, the expected error message should align with pkg/appender/ingest.go:308
					Value: "trying to ingest values of incompatible data type"}),
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
			appender.AddFast(ref, curr.Time, curr.Value)
		}

		if _, err := appender.WaitForCompletion(0); err != nil {
			if !isExpected(testParams, err) {
				t.Fatalf("Failed to wait for appender completion. reason: %s", err)
			}
		}

		expectedCount := len(dp.Data)
		if dp.ExpectedCount != nil {
			expectedCount = *dp.ExpectedCount
		}
		tsdbtest.ValidateCountOfSamples(t, adapter, dp.Name, expectedCount, from, to, -1)
	}
}

func isExpected(testParams tsdbtest.TestParams, actualErr error) bool {
	if errMsg, ok := testParams["expected_error_contains_string"]; ok {
		return strings.Contains(actualErr.Error(), fmt.Sprintf("%v", errMsg))
	}
	return false
}

func TestIngestDataWithSameTimestamp(t *testing.T) {
	baseTime := int64(1532209200000)
	testParams := tsdbtest.NewTestParams(t,
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data: []tsdbtest.DataPoint{
					{Time: baseTime, Value: 1},
					{Time: baseTime, Value: 2}}},
				tsdbtest.Metric{
					Name:   "cpu1",
					Labels: utils.LabelsFromStringList("os", "linux"),
					Data: []tsdbtest.DataPoint{
						{Time: baseTime, Value: 2}, {Time: baseTime, Value: 3}}},
			}})

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
		labels := utils.Labels{utils.Label{Name: "__name__", Value: dp.Name}}
		labels = append(labels, dp.Labels...)

		ref, err := appender.Add(labels, dp.Data[0].Time, dp.Data[0].Value)
		if err != nil {
			t.Fatalf("Failed to add data to appender. reason: %s", err)
		}

		for _, curr := range dp.Data[1:] {
			appender.AddFast(ref, curr.Time, curr.Value)
		}

		if _, err := appender.WaitForCompletion(0); err != nil {
			t.Fatalf("Failed to wait for appender completion. reason: %s", err)
		}
	}

	tsdbtest.ValidateCountOfSamples(t, adapter, "", 2, baseTime-1*tsdbtest.HoursInMillis, baseTime+1*tsdbtest.HoursInMillis, -1)
}

// test for http://jira.iguazeng.com:8080/browse/IG-14978
func TestIngestWithTimeDeltaBiggerThen32Bit(t *testing.T) {
	data := []tsdbtest.DataPoint{
		{Time: 1384786967945, Value: 1.0},
		{Time: 1392818567945, Value: 2.0}}
	testParams := tsdbtest.NewTestParams(t,
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: utils.LabelsFromStringList("os", "linux"),
				Data:   data},
			}})

	schema, err := schema.NewSchema(testParams.V3ioConfig(), "1/h", "1h", "", "")
	defer tsdbtest.SetUpWithDBConfig(t, schema, testParams)()

	adapter, err := NewV3ioAdapter(testParams.V3ioConfig(), nil, nil)
	if err != nil {
		t.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatalf("Failed to get appender. reason: %s", err)
	}

	for _, dp := range testParams.TimeSeries() {
		labels := utils.Labels{utils.Label{Name: "__name__", Value: dp.Name}}
		labels = append(labels, dp.Labels...)

		ref, err := appender.Add(labels, dp.Data[0].Time, dp.Data[0].Value)
		if err != nil {
			t.Fatalf("Failed to add data to appender. reason: %s", err)
		}

		for _, curr := range dp.Data[1:] {
			appender.AddFast(ref, curr.Time, curr.Value)
		}

		if _, err := appender.WaitForCompletion(0); err != nil {
			t.Fatalf("Failed to wait for appender completion. reason: %s", err)
		}
	}

	querier, _ := adapter.QuerierV2()
	iter, _ := querier.Select(&pquerier.SelectParams{From: 0,
		To: time.Now().Unix() * 1000})
	for iter.Next() {
		dataIter := iter.At().Iterator()
		actual, err := iteratorToSlice(dataIter)
		if err != nil {
			t.Fatal(err)
		}

		assert.ElementsMatch(t, data, actual,
			"result data didn't match. \nExpected: %v\n Actual: %v", data, actual)
	}

	if iter.Err() != nil {
		t.Fatal(err)
	}
}

func TestIngestVarTypeWithTimeDeltaBiggerThen32Bit(t *testing.T) {
	data := []string{"a", "b"}
	times := []int64{1384786967945, 1392818567945}

	testParams := tsdbtest.NewTestParams(t)

	schema, err := schema.NewSchema(testParams.V3ioConfig(), "1/h", "1h", "", "")
	defer tsdbtest.SetUpWithDBConfig(t, schema, testParams)()

	adapter, err := NewV3ioAdapter(testParams.V3ioConfig(), nil, nil)
	if err != nil {
		t.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatalf("Failed to get appender. reason: %s", err)
	}

	labels := utils.Labels{utils.Label{Name: "__name__", Value: "metric_1"}}
	for i, v := range data {
		_, err := appender.Add(labels, times[i], v)
		if err != nil {
			t.Fatalf("Failed to add data to appender. reason: %s", err)
		}

	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		t.Fatalf("Failed to wait for appender completion. reason: %s", err)
	}

	querier, _ := adapter.QuerierV2()
	iter, _ := querier.Select(&pquerier.SelectParams{From: 0,
		To: time.Now().Unix() * 1000})
	var seriesCount int
	for iter.Next() {
		seriesCount++
		iter := iter.At().Iterator()
		var i int
		for iter.Next() {
			time, value := iter.AtString()
			assert.Equal(t, times[i], time, "time does not match at index %v", i)
			assert.Equal(t, data[i], value, "value does not match at index %v", i)
			i++
		}
	}

	assert.Equal(t, 1, seriesCount, "series count didn't match expected")

	if iter.Err() != nil {
		t.Fatal(err)
	}
}

func TestWriteMetricWithDashInName(t *testing.T) {
	testParams := tsdbtest.NewTestParams(t,
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu-1",
				Labels: utils.LabelsFromStringList("testLabel", "balbala"),
				Data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}},
			}}})
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
		labels := utils.Labels{utils.Label{Name: "__name__", Value: dp.Name}}
		labels = append(labels, dp.Labels...)

		_, err := appender.Add(labels, dp.Data[0].Time, dp.Data[0].Value)
		if err == nil {
			t.Fatalf("Test should have failed")
		}
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

			for _, data := range expected[currentAggregate] {
				var equalCount = 0
				for _, dp := range actual {
					if dp.Equals(data) {
						equalCount++
						continue
					}
				}
				assert.Equal(test, equalCount, len(expected[currentAggregate]),
					"Check failed for aggregate='%s'. Query aggregates: %s", currentAggregate, queryAggregates)
			}
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
			var equalCount = 0
			for _, dp := range actual {
				if dp.Equals(data) {
					equalCount++
					continue
				}
			}
			assert.Equal(test, equalCount, len(expected[agg]))
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

		for _, data := range expected[agg] {
			var equalCount = 0
			for _, dp := range actual {
				if dp.Equals(data) {
					equalCount++
					continue
				}
			}
			assert.Equal(t, equalCount, len(expected[agg]))
		}
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
	if err := CreateTSDB(v3ioConfig, schema, nil); err != nil {
		v3ioConfigAsJson, _ := json.MarshalIndent(v3ioConfig, "", "  ")
		t.Fatalf("Failed to create TSDB. Reason: %s\nConfiguration:\n%s", err, string(v3ioConfigAsJson))
	}

	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}
	responseChan := make(chan *v3io.Response)
	container, _ := adapter.GetContainer()
	_, err = container.GetContainerContents(&v3io.GetContainerContentsInput{Path: v3ioConfig.TablePath}, 30, responseChan)
	if err != nil {
		t.Fatal(err.Error())
	}
	if res := <-responseChan; res.Error != nil {
		t.Fatal(res.Error.Error())
	}

	if err := adapter.DeleteDB(DeleteParams{DeleteAll: true, IgnoreErrors: true}); err != nil {
		t.Fatalf("Failed to delete DB on teardown. reason: %s", err)
	}

	_, err = container.GetContainerContents(&v3io.GetContainerContentsInput{Path: v3ioConfig.TablePath}, 30, responseChan)
	if err != nil {
		t.Fatal(err.Error())
	}
	if res := <-responseChan; res.Error == nil {
		t.Fatal("Did not delete TSDB properly")
	}
}

func TestIngestDataFloatThenString(t *testing.T) {
	testParams := tsdbtest.NewTestParams(t)

	defer tsdbtest.SetUp(t, testParams)()

	adapter, err := NewV3ioAdapter(testParams.V3ioConfig(), nil, nil)
	if err != nil {
		t.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatalf("Failed to get appender. reason: %s", err)
	}

	labels := utils.Labels{utils.Label{Name: "__name__", Value: "cpu"}}
	_, err = appender.Add(labels, 1532940510000, 12.0)
	if err != nil {
		t.Fatalf("Failed to add data to appender. reason: %s", err)
	}

	_, err = appender.Add(labels, 1532940610000, "tal")
	if err == nil {
		t.Fatal("expected failure but finished successfully")
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		t.Fatalf("Failed to wait for appender completion. reason: %s", err)
	}

	tsdbtest.ValidateCountOfSamples(t, adapter, "cpu", 1, 0, 1532950510000, -1)
}

func TestIngestDataStringThenFloat(t *testing.T) {
	testParams := tsdbtest.NewTestParams(t)

	defer tsdbtest.SetUp(t, testParams)()

	adapter, err := NewV3ioAdapter(testParams.V3ioConfig(), nil, nil)
	if err != nil {
		t.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatalf("Failed to get appender. reason: %s", err)
	}

	labels := utils.Labels{utils.Label{Name: "__name__", Value: "cpu"}}
	_, err = appender.Add(labels, 1532940510000, "tal")
	if err != nil {
		t.Fatalf("Failed to add data to appender. reason: %s", err)
	}

	_, err = appender.Add(labels, 1532940610000, 666.0)
	if err == nil {
		t.Fatal("expected failure but finished successfully")
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		t.Fatalf("Failed to wait for appender completion. reason: %s", err)
	}

	tsdbtest.ValidateCountOfSamples(t, adapter, "cpu", 1, 0, 1532950510000, -1)
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
