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
	"fmt"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/config"
	. "github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"path/filepath"
	"reflect"
	"testing"
)

func TestIngestData(t *testing.T) {
	v3ioConfig, err := config.LoadConfig(filepath.Join("../../", config.DefaultConfigurationFileName))
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

		{desc: "Should ingest record with late arrival", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3},
				{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 - 10, Value: 3234.6}}},

		{desc: "Should ingest record with '-' in the metric name (IG-8585)", metricName: "cool-cpu",
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

	responseChan := make(chan *v3io.Response)
	container, _ := adapter.GetContainer()
	container.GetItems(&v3io.GetItemsInput{
		Path:           fmt.Sprintf("/%s/0/", v3ioConfig.Path),
		AttributeNames: []string{"*"},
	}, 30, responseChan)

	res := <-responseChan
	getItemsResp := res.Output.(*v3io.GetItemsOutput)

	for _, item := range getItemsResp.Items {
		for _, label := range userLabels {
			actual := item.GetField(label.Name)
			if actual != label.Value {
				t.Fatalf("Records were not saved correctly for label %s, actual: %v, expected: %v",
					label.Name, actual, label.Value)
			}
		}
	}
}

func TestQueryData(t *testing.T) {
	v3ioConfig, err := config.LoadConfig(filepath.Join("../../", config.DefaultConfigurationFileName))
	if err != nil {
		t.Fatalf("Failed to load test configuration. reason: %s", err)
	}

	testCases := []struct {
		desc         string
		metricName   string
		labels       []utils.Label
		data         []tsdbtest.DataPoint
		filter       string
		from         int64
		to           int64
		expected     []tsdbtest.DataPoint
		ignoreReason string
	}{
		{desc: "Should ingest and query one data point", metricName: "cpu",
			labels: utils.FromStrings("testLabel", "balbala"),
			data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}},
			from:   0, to: 1532940510 + 1,
			expected: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}}},

		{desc: "Should ingest and query multiple data points", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []tsdbtest.DataPoint{{Time: 1532940510 - 10, Value: 314.3},
				{Time: 1532940510 - 5, Value: 300.3},
				{Time: 1532940510, Value: 3234.6}},
			from: 0, to: 1532940510 + 1,
			expected: []tsdbtest.DataPoint{{Time: 1532940510 - 10, Value: 314.3},
				{Time: 1532940510 - 5, Value: 300.3},
				{Time: 1532940510, Value: 3234.6}}},

		{desc: "Should query with filter on metric name", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 33.3}},
			filter: "_name=='cpu'",
			from:   0, to: 1532940510 + 1,
			expected: []tsdbtest.DataPoint{{Time: 1532940510, Value: 33.3}}},

		{desc: "Should query with filter on label name", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 31.3}},
			filter: "os=='linux'",
			from:   0, to: 1532940510 + 1,
			expected: []tsdbtest.DataPoint{{Time: 1532940510, Value: 31.3}}},

		{desc: "Should ingest and query data with '-' in the metric name (IG-8585)", metricName: "cool-cpu",
			labels: utils.FromStrings("testLabel", "balbala"),
			data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}},
			from:   0, to: 1532940510 + 1,
			expected: []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}}},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testQueryDataCase(t, v3ioConfig, test.metricName, test.labels,
				test.data, test.filter, test.from, test.to, test.expected)
		})
	}
}

func testQueryDataCase(test *testing.T, v3ioConfig *config.V3ioConfig,
	metricsName string, userLabels []utils.Label, data []tsdbtest.DataPoint, filter string,
	from int64, to int64, expected []tsdbtest.DataPoint) {
	defer tsdbtest.SetUp(test, v3ioConfig)()

	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		test.Fatalf("Failed to create v3io adapter. reason: %s", err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		test.Fatalf("Failed to get appender. reason: %s", err)
	}

	labels := utils.Labels{utils.Label{Name: "__name__", Value: metricsName}}
	labels = append(labels, userLabels...)

	ref, err := appender.Add(labels, data[0].Time, data[0].Value)
	if err != nil {
		test.Fatalf("Failed to add data to appender. reason: %s", err)
	}
	for _, curr := range data[1:] {
		appender.AddFast(labels, ref, curr.Time, curr.Value)
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		test.Fatalf("Failed to wait for appender completion. reason: %s", err)
	}

	qry, err := adapter.Querier(nil, from, to)
	if err != nil {
		test.Fatalf("Failed to create Querier. reason: %v", err)
	}

	set, err := qry.Select(metricsName, "", 1000, filter)
	if err != nil {
		test.Fatalf("Failed to run Select. reason: %v", err)
	}

	counter := 0
	for set.Next() {
		if set.Err() != nil {
			test.Fatalf("Failed to query metric. reason: %v", set.Err())
		}

		series := set.At()
		iter := series.Iterator()
		if iter.Err() != nil {
			test.Fatalf("Failed to query data series. reason: %v", iter.Err())
		}
		for _, expected := range expected {
			if !iter.Next() {
				test.Fatalf("Number of actual data points (%d) is less the expected (%d)", counter, len(data))
			}
			t, v := iter.At()

			fmt.Printf("(%v, %v)\n", t, v)
			if t != expected.Time || v != expected.Value {
				test.Fatalf("actual: (t=%v, v=%v) is not equal to expected:(t=%v, v=%v)   === %f",
					t, v, expected.Time, expected.Value, v-expected.Value)
			}

			counter++
		}
	}

	if counter == 0 {
		test.Fatalf("No data was recieved")
	}
}

func TestCreateTSDB(t *testing.T) {
	v3ioConfig, err := config.LoadConfig(filepath.Join("../../", config.DefaultConfigurationFileName))
	if err != nil {
		t.Fatalf("Failed to load test configuration. reason: %s", err)
	}

	testCases := []struct {
		desc         string
		conf         config.DBPartConfig
		ignoreReason string
	}{
		{desc: "Should create TSDB with standard configuration", conf: config.DBPartConfig{
			Signature:      "TSDB",
			Version:        "1.0",
			DaysPerObj:     1,
			HrInChunk:      1,
			DefaultRollups: "count,sum",
			RollupMin:      10,
		}},

		{desc: "Should create TSDB with wildcard aggregations", conf: config.DBPartConfig{
			Signature:      "TSDB",
			Version:        "1.0",
			DaysPerObj:     1,
			HrInChunk:      1,
			DefaultRollups: "*",
			RollupMin:      10,
		}},
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

func testCreateTSDBcase(t *testing.T, v3ioConfig *config.V3ioConfig, dbConfig config.DBPartConfig) {
	defer tsdbtest.SetUpWithDBConfig(t, v3ioConfig, dbConfig)()

	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create adapter. reason: %s", err)
	}

	actualDbConfig := *adapter.GetDBConfig()

	if !reflect.DeepEqual(actualDbConfig, dbConfig) {
		t.Fatalf("actual: %v is not equal to expected: %v", actualDbConfig, dbConfig)
	}
}
