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

package tsdb

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"testing"
	"time"
	"path/filepath"
	"github.com/v3io/v3io-go-http"
	"reflect"
)

func deleteTSDB(t *testing.T, v3ioConfig *config.V3ioConfig) {
	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create adapter. reason: %s", err)
	}

	if err := adapter.DeleteDB(true, true); err != nil {
		t.Fatalf("Failed to delete DB on teardown. reason: %s", err)
	}
}

func setUp(t *testing.T, v3ioConfig *config.V3ioConfig) func() {
	dbConfig := config.DBPartConfig{
		DaysPerObj:     1,
		HrInChunk:      1,
		DefaultRollups: "sum",
		RollupMin:      10,
	}

	if err := CreateTSDB(v3ioConfig, &dbConfig); err != nil {
		t.Fatalf("Failed to create TSDB. reason: %s", err)
	}

	return func() {
		deleteTSDB(t, v3ioConfig)
	}
}

func setUpWithDBConfig(t *testing.T, v3ioConfig *config.V3ioConfig, dbConfig config.DBPartConfig) func() {
	if err := CreateTSDB(v3ioConfig, &dbConfig); err != nil {
		t.Fatalf("Failed to create TSDB. reason: %s", err)
	}

	return func() {
		deleteTSDB(t, v3ioConfig)
	}
}

type testDataPoint struct {
	t int64
	v float64
}

func TestIngestData(t *testing.T) {
	v3ioConfig, err := config.LoadConfig(filepath.Join("../../", config.DefaultConfigurationFileName))
	if err != nil {
		t.Fatalf("Failed to load test configuration. reason: %s", err)
	}

	testCases := []struct {
		desc       string
		metricName string
		labels     []utils.Label
		data       []testDataPoint
	}{
		{desc: "one data point", metricName: "cpu", labels: utils.FromStrings("testLabel", "balbala"),
			data: []testDataPoint{{t: time.Now().Unix(), v: 314.3}}},

		{desc: "multiple data points", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []testDataPoint{{t: time.Now().Unix(), v: 314.3},
				{t: time.Now().Unix() + 5, v: 300.3},
				{t: time.Now().Unix() + 10, v: 3234.6}}},

		{desc: "late arrival", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []testDataPoint{{t: time.Now().Unix(), v: 314.3},
				{t: time.Now().Unix() + 5, v: 300.3},
				{t: time.Now().Unix() - 10, v: 3234.6}}},
	}

	for _, test := range testCases {
		t.Logf("%s\n", test.desc)
		testIngestDataCase(t, v3ioConfig, test.desc, test.metricName, test.labels, test.data)
	}
}

func testIngestDataCase(t *testing.T, v3ioConfig *config.V3ioConfig, testDescription string,
	metricsName string, userLabels []utils.Label, data []testDataPoint) {
	defer setUp(t, v3ioConfig)()

	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("test %s - Failed to create v3io adapter. reason: %s", testDescription, err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		t.Fatalf("test %s - Failed to get appender. reason: %s", testDescription, err)
	}

	labels := utils.Labels{utils.Label{Name: "__name__", Value: metricsName}}
	labels = append(labels, userLabels...)

	fmt.Printf("the labeles are: %v\n", labels)
	ref, err := appender.Add(labels, data[0].t, data[0].v)
	if err != nil {
		t.Fatalf("test %s - Failed to add data to appender. reason: %s", testDescription, err)
	}
	for i := 1; i < len(data); i++ {
		appender.AddFast(labels, ref, data[i].t, data[i].v)
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		t.Fatalf("test %s - Failed to wait for appender completion. reason: %s", testDescription, err)
	}

	responseChan := make(chan *v3io.Response)
	adapter.container.GetItems(&v3io.GetItemsInput{
		Path:           "/metrics/0/",
		AttributeNames: []string{"*"},
	}, 30, responseChan)

	res := <-responseChan
	getItemsResp := res.Output.(*v3io.GetItemsOutput)

	for _, item := range getItemsResp.Items {
		for _, label := range userLabels {
			actual := item.GetField(label.Name)
			if actual != label.Value {
				t.Fatalf("test %s - Records were not saved correctly. for label %s, actual: %v, expected: %v",
					testDescription, label.Name, actual, label.Value)
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
		desc       string
		metricName string
		labels     []utils.Label
		data       []testDataPoint
		filter     string
		from       int64
		to         int64
		expected   []testDataPoint
	}{
		{desc: "one data point", metricName: "cpu",
			labels: utils.FromStrings("testLabel", "balbala"),
			data: []testDataPoint{{t: 1532940510, v: 314.3}},
			from: 0, to: time.Now().Unix() + 1,
			expected: []testDataPoint{{t: 1532940510, v: 314.3}}},

		{desc: "multiple data points", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []testDataPoint{{t: 1532940510 - 10, v: 314.3},
				{t: 1532940510 - 5, v: 300.3},
				{t: 1532940510, v: 3234.6}},
			from: 0, to: time.Now().Unix() + 1,
			expected: []testDataPoint{{t: 1532940510 - 10, v: 314.3},
				{t: 1532940510 - 5, v: 300.3},
				{t: 1532940510, v: 3234.6}}},

		{desc: "filter on metric name", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []testDataPoint{{t: 1532940510, v: 33.3}},
			filter: "_name=='cpu'",
			from: 0, to: time.Now().Unix() + 1,
			expected: []testDataPoint{{t: 1532940510, v: 33.3}}},

		{desc: "filter on label name", metricName: "cpu",
			labels: utils.FromStrings("os", "linux", "iguaz", "yesplease"),
			data: []testDataPoint{{t: 1532940510, v: 31.3}},
			filter: "os=='linux'",
			from: 0, to: time.Now().Unix() + 1,
			expected: []testDataPoint{{t: 1532940510, v: 31.3}}},
	}

	for _, test := range testCases {
		t.Logf("%s\n", test.desc)
		testQueryDataCase(t, v3ioConfig, test.desc, test.metricName, test.labels,
			test.data, test.filter, test.from, test.to, test.expected)
	}
}

func testQueryDataCase(test *testing.T, v3ioConfig *config.V3ioConfig, testDescription string,
	metricsName string, userLabels []utils.Label, data []testDataPoint, filter string,
	from int64, to int64, expected []testDataPoint) {
	defer setUp(test, v3ioConfig)()

	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		test.Fatalf("test %s - Failed to create v3io adapter. reason: %s", testDescription, err)
	}

	appender, err := adapter.Appender()
	if err != nil {
		test.Fatalf("test %s - Failed to get appender. reason: %s", testDescription, err)
	}

	labels := utils.Labels{utils.Label{Name: "__name__", Value: metricsName}}
	labels = append(labels, userLabels...)

	ref, err := appender.Add(labels, data[0].t, data[0].v)
	if err != nil {
		test.Fatalf("test %s - Failed to add data to appender. reason: %s", testDescription, err)
	}
	for i := 1; i < len(data); i++ {
		appender.AddFast(labels, ref, data[i].t, data[i].v)
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		test.Fatalf("test %s - Failed to wait for appender completion. reason: %s", testDescription, err)
	}

	qry, err := adapter.Querier(nil, from, to)
	if err != nil {
		test.Fatalf("test %s - Failed to create Querier. reason: %v", testDescription, err)
	}

	set, err := qry.Select(metricsName, "", 1000, filter)
	//set, err := qry.Select("count,avg,sum", 1000*3600, "_name=='http_req'")
	//set, err := qry.SelectOverlap("count,avg,sum,max", 1000*3600, []int{4, 2, 1}, "_name=='http_req'")
	if err != nil {
		test.Fatalf("test %s - Failed to run Select. reason: %v", testDescription, err)
	}

	counter := 0
	for set.Next() {
		if set.Err() != nil {
			test.Fatalf("test %s - Failed to query metric. reason: %v", testDescription, set.Err())
		}

		series := set.At()
		iter := series.Iterator()
		if iter.Err() != nil {
			test.Fatalf("test %s - Failed to query data series. reason: %v", testDescription, iter.Err())
		}
		for _, expected := range expected {
			if !iter.Next() {
				test.Fatalf("test %s - Number of actual data points (%d) is less the expected (%d)", testDescription, counter, len(data))
			}
			t, v := iter.At()

			fmt.Printf("(%v, %v)\n", t, v)
			if t != expected.t || v != expected.v {
				test.Fatalf("actual: (t=%v, v=%v) is not equal to expected:(t=%v, v=%v)   === %f",
					t, v, expected.t, expected.v, v-expected.v)
			}

			counter++
		}
	}

	if counter == 0 {
		test.Fatalf("test %s - No data was recieved", testDescription)
	}
}

func TestCreateTSDB(t *testing.T) {
	v3ioConfig, err := config.LoadConfig(filepath.Join("../../", config.DefaultConfigurationFileName))
	if err != nil {
		t.Fatalf("Failed to load test configuration. reason: %s", err)
	}

	testCases := []struct {
		desc string
		conf config.DBPartConfig
	}{
		{"standard configuration", config.DBPartConfig{
			Signature:      "TSDB",
			Version:        "1.0",
			DaysPerObj:     1,
			HrInChunk:      1,
			DefaultRollups: "count,sum",
			RollupMin:      10,
		}},

		{"asterix aggregations", config.DBPartConfig{
			Signature:      "TSDB",
			Version:        "1.0",
			DaysPerObj:     1,
			HrInChunk:      1,
			DefaultRollups: "*",
			RollupMin:      10,
		}},
	}

	for _, test := range testCases {
		t.Logf("%s\n", test.desc)
		testCreateTSDBcase(t, v3ioConfig, test.desc, test.conf)
	}

}

func testCreateTSDBcase(t *testing.T, v3ioConfig *config.V3ioConfig, testDescription string, dbConfig config.DBPartConfig) {
	defer setUpWithDBConfig(t, v3ioConfig, dbConfig)()

	adapter, err := NewV3ioAdapter(v3ioConfig, nil, nil)
	if err != nil {
		t.Fatalf("test %s - Failed to create adapter. reason: %s", testDescription, err)
	}

	actualDbConfig := *adapter.GetDBConfig()

	if !reflect.DeepEqual(actualDbConfig, dbConfig) {
		t.Fatalf("test %s - actual: %v is not equal to expected: %v", testDescription, actualDbConfig, dbConfig)
	}
}
