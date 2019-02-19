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

package pquerier_test

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type testQuerySuite struct {
	suite.Suite
	v3ioConfig     *config.V3ioConfig
	suiteTimestamp int64
	basicQueryTime int64
}

func (suite *testQuerySuite) SetupSuite() {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		suite.T().Fatalf("unable to load configuration. Error: %v", err)
	}

	suite.v3ioConfig = v3ioConfig
	suite.suiteTimestamp = time.Now().Unix()
	suite.basicQueryTime = suite.toMillis("2018-07-21T21:40:00Z")
}

func (suite *testQuerySuite) SetupTest() {
	suite.v3ioConfig.TablePath = fmt.Sprintf("%s-%v", suite.T().Name(), suite.suiteTimestamp)
	tsdbtest.CreateTestTSDB(suite.T(), suite.v3ioConfig)
}

func (suite *testQuerySuite) TearDownTest() {
	suite.v3ioConfig.TablePath = fmt.Sprintf("%s-%v", suite.T().Name(), suite.suiteTimestamp)
	if !suite.T().Failed() {
		tsdbtest.DeleteTSDB(suite.T(), suite.v3ioConfig)
	}
}

func (suite *testQuerySuite) TestRawDataSinglePartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	expectedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   expectedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   expectedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expectedData, data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 2, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestRawDataMultiplePartitions() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	numberOfEvents := 5
	eventsInterval := int64(tsdbtest.MinuteInMillis)
	expectedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*eventsInterval, 30},
		{suite.basicQueryTime + 3*eventsInterval, 40}}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   expectedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   expectedData},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", From: suite.basicQueryTime - 8*tsdbtest.DaysInMillis, To: suite.basicQueryTime + int64(numberOfEvents)*eventsInterval}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expectedData, data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 2, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestFilterOnLabel() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	numberOfEvents := 5
	eventsInterval := int64(tsdbtest.MinuteInMillis)
	expectedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*eventsInterval, 30},
		{suite.basicQueryTime + 3*eventsInterval, 40}}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   expectedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   expectedData},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Filter: "os=='linux'",
		From: suite.basicQueryTime - 8*tsdbtest.DaysInMillis, To: suite.basicQueryTime + int64(numberOfEvents)*eventsInterval}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expectedData, data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestQueryWithBadTimeParameters() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	expectedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   expectedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   expectedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", From: suite.basicQueryTime + int64(numberOfEvents*eventsInterval), To: suite.basicQueryTime}
	_, err = querierV2.Select(params)
	if err == nil {
		suite.T().Fatalf("expected to get error but no error was returned")
	}
}

func (suite *testQuerySuite) TestQueryMetricWithDashInTheName() { // IG-8585
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	expectedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cool-cpu",
				Labels: labels1,
				Data:   expectedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	_, err = querierV2.Select(params)
	if err == nil {
		suite.T().Fatalf("expected an error but finish succesfully")
	}
}

func (suite *testQuerySuite) TestQueryAggregateWithNameWildcard() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestData},
				tsdbtest.Metric{
					Name:   "diskio",
					Labels: labels1,
					Data:   ingestData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedData := map[string][]tsdbtest.DataPoint{
		"sum": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: suite.basicQueryTime, Value: 20}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: suite.basicQueryTime, Value: 20}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 30}},
		"max": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: suite.basicQueryTime, Value: 20}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 40}}}
	expected := map[string]map[string][]tsdbtest.DataPoint{"cpu": expectedData, "diskio": expectedData}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Functions: "max,min,sum", Step: 2 * tsdbtest.MinuteInMillis,
		From: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		metricName := set.At().Labels().Get(config.PrometheusMetricNameAttribute)
		aggr := set.At().Labels().Get(aggregate.AggregateLabel)
		iter := set.At().Iterator()
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[metricName][aggr], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), len(expectedData)*len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestQueryAggregateWithFilterOnMetricName() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestData},
				tsdbtest.Metric{
					Name:   "diskio",
					Labels: labels1,
					Data:   ingestData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedData := map[string][]tsdbtest.DataPoint{"max": {{Time: suite.basicQueryTime, Value: 20}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 40}}}
	expected := map[string]map[string][]tsdbtest.DataPoint{"cpu": expectedData}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Functions: "max", Step: 2 * tsdbtest.MinuteInMillis,
		From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval), Filter: "_name=='cpu'"}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		metricName := set.At().Labels().Get(config.PrometheusMetricNameAttribute)
		aggr := set.At().Labels().Get(aggregate.AggregateLabel)
		iter := set.At().Iterator()
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[metricName][aggr], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestDownSampleNotReturningAggrAttr() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 6*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 9*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Step: 2 * int64(tsdbtest.MinuteInMillis), From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		labels := set.At().Labels()
		suite.Require().Empty(labels.Get(aggregate.AggregateLabel))
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestRawDataSinglePartitionWithDownSample() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 6*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 9*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expectedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{suite.basicQueryTime + 6*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 8*tsdbtest.MinuteInMillis, 40}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Step: 2 * int64(tsdbtest.MinuteInMillis), From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expectedData, data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestRawDataDownSampleMultiPartitions() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")

	ingestData := []tsdbtest.DataPoint{{suite.toMillis("2018-11-18T23:40:00Z"), 10},
		{suite.toMillis("2018-11-18T23:59:00Z"), 20},
		{suite.toMillis("2018-11-19T00:20:00Z"), 30},
		{suite.toMillis("2018-11-19T02:40:00Z"), 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expectedData := []tsdbtest.DataPoint{{suite.toMillis("2018-11-18T22:00:00Z"), 10},
		{suite.toMillis("2018-11-19T00:00:00Z"), 30},
		{suite.toMillis("2018-11-19T02:00:00Z"), 40}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu"}},
		Step: 2 * int64(tsdbtest.HoursInMillis),
		From: suite.toMillis("2018-11-18T22:00:00Z"),
		To:   suite.toMillis("2018-11-19T4:00:00Z")}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		data, err := tsdbtest.IteratorToSlice(iter)

		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expectedData, data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestRawAggregatesSinglePartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: suite.basicQueryTime, Value: 100}},
		"min": {{Time: suite.basicQueryTime, Value: 10}},
		"max": {{Time: suite.basicQueryTime, Value: 40}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min", Step: 1 * 60 * 60 * 1000, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestRawAggregatesWithQueryIntervalSameAsAggrGranularity() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	baseTime := suite.toMillis("2018-07-21T21:00:00Z")
	ingestedData := []tsdbtest.DataPoint{{baseTime, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*tsdbtest.MinuteInMillis, 30},
		{baseTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: baseTime, Value: 100}},
		"min": {{Time: baseTime, Value: 10}},
		"max": {{Time: baseTime, Value: 40}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min", Step: 1 * 60 * 60 * 1000, From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestRawAggregatesSinglePartitionNegativeValues() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, -10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), -20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, -30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, -40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: suite.basicQueryTime, Value: -100}},
		"min": {{Time: suite.basicQueryTime, Value: -40}},
		"max": {{Time: suite.basicQueryTime, Value: -10}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min", Step: 1 * 60 * 60 * 1000, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestRawAggregatesMultiPartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")

	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: suite.basicQueryTime, Value: 90}},
		"min": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: suite.basicQueryTime, Value: 20}},
		"max": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: suite.basicQueryTime, Value: 40}},
		"sqr": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 100}, {Time: suite.basicQueryTime, Value: 2900}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min,sqr", Step: 1 * 60 * 60 * 1000, From: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestRawAggregatesMultiPartitionNonConcreteAggregates() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{suite.basicQueryTime - 7*tsdbtest.DaysInMillis + tsdbtest.MinuteInMillis, 12},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"avg": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 11}, {Time: suite.basicQueryTime, Value: 30}},
		"stdvar": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 2}, {Time: suite.basicQueryTime, Value: 100}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "avg,stdvar", Step: 1 * 60 * 60 * 1000, From: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestClientAggregatesSinglePartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: suite.basicQueryTime, Value: 30}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: suite.basicQueryTime, Value: 10}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 30}},
		"max": {{Time: suite.basicQueryTime, Value: 20}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 40}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min", Step: 2 * 60 * 1000, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestClientAggregatesMultiPartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(suite.basicQueryTime), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: suite.basicQueryTime, Value: 90}},
		"min": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: suite.basicQueryTime, Value: 20}},
		"max": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: suite.basicQueryTime, Value: 40}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "sum,max,min",
		Step:      5 * tsdbtest.MinuteInMillis,
		From:      suite.basicQueryTime - 7*tsdbtest.DaysInMillis,
		To:        suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestClientAggregatesMultiPartitionNonConcreteAggregates() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{suite.basicQueryTime - 7*tsdbtest.DaysInMillis + tsdbtest.MinuteInMillis, 12},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"avg": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 11}, {Time: suite.basicQueryTime, Value: 30}},
		"stdvar": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 2}, {Time: suite.basicQueryTime, Value: 100}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "avg,stdvar",
		Step:      5 * tsdbtest.MinuteInMillis,
		From:      suite.basicQueryTime - 7*tsdbtest.DaysInMillis,
		To:        suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestClientAggregatesMultiPartitionOneStep() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 25*tsdbtest.DaysInMillis, 10},
		{suite.basicQueryTime - 20*tsdbtest.DaysInMillis, 20},
		{suite.basicQueryTime - 12*tsdbtest.DaysInMillis, 30},
		{suite.basicQueryTime - 1*tsdbtest.DaysInMillis, 40},
		{suite.basicQueryTime + 20*tsdbtest.DaysInMillis, 50}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"count": {{Time: suite.basicQueryTime - 25*tsdbtest.DaysInMillis, Value: 5}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "count",
		Step:      0,
		From:      suite.basicQueryTime - 25*tsdbtest.DaysInMillis,
		To:        suite.basicQueryTime + 21*tsdbtest.DaysInMillis}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestGetEmptyResponse() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "i dont exist", Functions: "sum,max,min,sqr", Step: 1 * 60 * 60 * 1000, From: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestSelectAggregatesByRequestedColumns() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: suite.basicQueryTime, Value: 30}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: suite.basicQueryTime, Value: 10}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 30}},
		"max": {{Time: suite.basicQueryTime, Value: 20}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 40}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu", Function: "max"}, {Metric: "cpu", Function: "min"}, {Metric: "cpu", Function: "sum"}},
		Step: 2 * 60 * 1000, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}

	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestSelectRawDataByRequestedColumns() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := ingestedData

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu"}},
		From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected, data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestSelectAggregatesAndRawByRequestedColumns() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: suite.basicQueryTime, Value: 30}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 70}},
		"": {{suite.basicQueryTime, 10}, {suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu", Function: "sum"}, {Metric: "cpu"}},
		Step: 2 * 60 * 1000, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}

	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 2, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestSelectServerAggregatesAndRawByRequestedColumns() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: suite.basicQueryTime, Value: 100}},
		"": {{suite.basicQueryTime, 10}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu", Function: "sum"}, {Metric: "cpu", Interpolator: "next"}},
		Step: 60 * tsdbtest.MinuteInMillis, From: suite.basicQueryTime, To: suite.basicQueryTime + 5*tsdbtest.MinuteInMillis}

	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 2, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestRawDataMultipleMetrics() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	metricName1 := "cpu"
	metricName2 := "diskio"
	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 5
	eventsInterval := int64(tsdbtest.MinuteInMillis)
	ingestData1 := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*eventsInterval, 30},
		{suite.basicQueryTime + 4*eventsInterval, 40}}
	ingestData2 := []tsdbtest.DataPoint{{suite.basicQueryTime - 5*tsdbtest.DaysInMillis, 10},
		{int64(suite.basicQueryTime + 2*tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 3*eventsInterval, 30},
		{suite.basicQueryTime + 4*eventsInterval, 40}}

	expectedData := map[string][]tsdbtest.DataPoint{metricName1: ingestData1, metricName2: ingestData2}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   metricName1,
				Labels: labels1,
				Data:   ingestData1},
				tsdbtest.Metric{
					Name:   metricName2,
					Labels: labels2,
					Data:   ingestData2},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: metricName1}, {Metric: metricName2}},
		From: suite.basicQueryTime - 8*tsdbtest.DaysInMillis, To: suite.basicQueryTime + int64(numberOfEvents)*eventsInterval}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		name := set.At().Labels().Get(config.PrometheusMetricNameAttribute)
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expectedData[name], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 2, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestDataFrameRawDataMultipleMetrics() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	metricName1 := "cpu"
	metricName2 := "diskio"
	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 5
	eventsInterval := int64(tsdbtest.MinuteInMillis)
	expectedTimeColumn := []int64{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, suite.basicQueryTime - 5*tsdbtest.DaysInMillis,
		suite.basicQueryTime + tsdbtest.MinuteInMillis, suite.basicQueryTime + 2*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, suite.basicQueryTime + 4*tsdbtest.MinuteInMillis}
	expectedColumns := map[string][]float64{metricName1: {10, math.NaN(), 20, 30, math.NaN(), 40},
		metricName2: {math.NaN(), 10, math.NaN(), 20, 30, 40}}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   metricName1,
				Labels: labels1,
				Data: []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
					{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
					{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
					{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 40}}},
				tsdbtest.Metric{
					Name:   metricName2,
					Labels: labels2,
					Data: []tsdbtest.DataPoint{{suite.basicQueryTime - 5*tsdbtest.DaysInMillis, 10},
						{int64(suite.basicQueryTime + 2*tsdbtest.MinuteInMillis), 20},
						{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 30},
						{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 40}}},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: metricName1}, {Metric: metricName2}},
		From: suite.basicQueryTime - 8*tsdbtest.DaysInMillis, To: suite.basicQueryTime + int64(numberOfEvents)*eventsInterval}
	iter, err := querierV2.SelectDataFrame(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}
	var seriesCount int
	for iter.NextFrame() {
		seriesCount++
		frame, err := iter.GetFrame()
		suite.NoError(err)
		indexCol := frame.Indices()[0] // in tsdb we have only one index

		for i := 0; i < indexCol.Len(); i++ {
			t, _ := indexCol.TimeAt(i)
			timeMillis := t.UnixNano() / int64(time.Millisecond)
			assert.Equal(suite.T(), expectedTimeColumn[i], timeMillis, "time column does not match at index %v", i)
			for _, columnName := range frame.Names() {
				column, err := frame.Column(columnName)
				suite.NoError(err)
				v, _ := column.FloatAt(i)

				expected := expectedColumns[column.Name()][i]

				// assert can not compare NaN, so we need to check it manually
				if !(math.IsNaN(expected) && math.IsNaN(v)) {
					assert.Equal(suite.T(), expectedColumns[column.Name()][i], v, "column %v does not match at index %v", column.Name(), i)
				}
			}
		}
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestQueryAllData() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: suite.basicQueryTime, Value: 30}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: suite.basicQueryTime, Value: 10}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 30}},
		"max": {{Time: suite.basicQueryTime, Value: 20}, {Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 40}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "sum,max,min",
		Step:      2 * 60 * 1000,
		From:      0,
		To:        math.MaxInt64}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestVariantTypeQueryWithDataFrame() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	metricName := "log"
	labels := utils.LabelsFromStringList("os", "linux", "__name__", metricName)

	dataToIngest := []string{"a", "b", "c", "d", "e"}
	numberOfEvents := len(dataToIngest)
	var expectedTimeColumn []int64
	for i := 0; i < numberOfEvents; i++ {
		expectedTimeColumn = append(expectedTimeColumn, suite.basicQueryTime+int64(i)*tsdbtest.MinuteInMillis)
	}

	appender, err := adapter.Appender()
	if err != nil {
		suite.T().Fatalf("failed to create v3io appender. reason: %s", err)
	}

	ref, err := appender.Add(labels, expectedTimeColumn[0], dataToIngest[0])
	if err != nil {
		suite.T().Fatalf("Failed to add data to the TSDB appender. Reason: %s", err)
	}
	for i := 1; i < numberOfEvents; i++ {
		appender.AddFast(labels, ref, expectedTimeColumn[i], dataToIngest[i])
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		suite.T().Fatalf("Failed to wait for TSDB append completion. Reason: %s", err)
	}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: metricName}},
		From: suite.basicQueryTime - tsdbtest.DaysInMillis, To: suite.basicQueryTime + tsdbtest.DaysInMillis}
	iter, err := querierV2.SelectDataFrame(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}
	var seriesCount int
	for iter.NextFrame() {
		seriesCount++
		frame, err := iter.GetFrame()
		suite.NoError(err)
		indexCol := frame.Indices()[0] // in tsdb we have only one index

		for i := 0; i < indexCol.Len(); i++ {
			t, _ := indexCol.TimeAt(i)
			timeMillis := t.UnixNano() / int64(time.Millisecond)
			assert.Equal(suite.T(), expectedTimeColumn[i], timeMillis, "time column does not match at index %v", i)
			for _, columnName := range frame.Names() {
				column, err := frame.Column(columnName)
				suite.NoError(err)
				v, _ := column.StringAt(i)

				expected := dataToIngest[i]

				assert.Equal(suite.T(), expected, v, "column %v does not match at index %v", column.Name(), i)
			}
		}
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestVariantTypeQueryWithSeries() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	metricName := "log"
	labels := utils.LabelsFromStringList("os", "linux", "__name__", metricName)

	dataToIngest := []string{"a", "b", "c", "d", "e"}
	numberOfEvents := len(dataToIngest)
	var expectedTimeColumn []int64
	for i := 0; i < numberOfEvents; i++ {
		expectedTimeColumn = append(expectedTimeColumn, suite.basicQueryTime+int64(i)*tsdbtest.MinuteInMillis)
	}

	appender, err := adapter.Appender()
	if err != nil {
		suite.T().Fatalf("failed to create v3io appender. reason: %s", err)
	}

	ref, err := appender.Add(labels, expectedTimeColumn[0], dataToIngest[0])
	if err != nil {
		suite.T().Fatalf("Failed to add data to the TSDB appender. Reason: %s", err)
	}
	for i := 1; i < numberOfEvents; i++ {
		appender.AddFast(labels, ref, expectedTimeColumn[i], dataToIngest[i])
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		suite.T().Fatalf("Failed to wait for TSDB append completion. Reason: %s", err)
	}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: metricName}},
		From: suite.basicQueryTime - tsdbtest.DaysInMillis, To: suite.basicQueryTime + tsdbtest.DaysInMillis}
	iter, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}
	var seriesCount int
	for iter.Next() {
		seriesCount++
		iter := iter.At().Iterator()
		var i int
		for iter.Next() {
			t, v := iter.AtString()
			assert.Equal(suite.T(), expectedTimeColumn[i], t, "time does not match at index %v", i)
			assert.Equal(suite.T(), dataToIngest[i], v, "value does not match at index %v", i)
			i++
		}
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestCrossSeriesAggregatesSinglePartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime, 20}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: suite.basicQueryTime, Value: 30}},
		"min":   {{Time: suite.basicQueryTime, Value: 10}},
		"max":   {{Time: suite.basicQueryTime, Value: 20}},
		"count": {{Time: suite.basicQueryTime, Value: 2}},
		"avg":   {{Time: suite.basicQueryTime, Value: 15}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum_all,min_all,max_all,count_all,avg_all", Step: 2 * 60 * 1000, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	suite.Require().NoError(err, "Failed to execute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.Require().Equal(expected[agg], data, "queried data does not match expected")
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestGroupByOneLabelSinglePartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux", "region", "europe")
	labels2 := utils.LabelsFromStringList("os", "mac", "region", "europe")
	labels3 := utils.LabelsFromStringList("os", "linux", "region", "americas")
	labels4 := utils.LabelsFromStringList("os", "linux", "region", "asia")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels3,
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels4,
					Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string]map[string][]tsdbtest.DataPoint{
		"linux": {
			"sum":   {{Time: suite.basicQueryTime, Value: 30}},
			"count": {{Time: suite.basicQueryTime, Value: 3}}},
		"mac": {
			"sum":   {{Time: suite.basicQueryTime, Value: 10}},
			"count": {{Time: suite.basicQueryTime, Value: 1}}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "sum,count",
		Step:      2 * 60 * 1000,
		From:      suite.basicQueryTime,
		To:        suite.basicQueryTime + int64(numberOfEvents*eventsInterval),
		GroupBy:   "os"}
	set, err := querierV2.Select(params)
	suite.Require().NoError(err, "failed to exeute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		groupByValue := set.At().Labels().Get("os")
		suite.Require().NoError(err)

		suite.Require().Equal(expected[groupByValue][agg], data, "queried data does not match expected")
	}

	suite.Require().Equal(4, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestGroupByMultipleLabelsSinglePartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux", "region", "europe", "version", "1")
	labels2 := utils.LabelsFromStringList("os", "linux", "region", "europe", "version", "2")
	labels3 := utils.LabelsFromStringList("os", "linux", "region", "americas", "version", "3")
	labels4 := utils.LabelsFromStringList("os", "mac", "region", "asia", "version", "1")
	labels5 := utils.LabelsFromStringList("os", "mac", "region", "asia", "version", "2")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels3,
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels4,
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels5,
					Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	groupBy := []string{"os", "region"}
	expected := map[string]map[string][]tsdbtest.DataPoint{
		"linux-europe": {
			"sum":   {{Time: suite.basicQueryTime, Value: 20}},
			"count": {{Time: suite.basicQueryTime, Value: 2}}},
		"linux-americas": {
			"sum":   {{Time: suite.basicQueryTime, Value: 10}},
			"count": {{Time: suite.basicQueryTime, Value: 1}}},
		"mac-asia": {
			"sum":   {{Time: suite.basicQueryTime, Value: 20}},
			"count": {{Time: suite.basicQueryTime, Value: 2}}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "sum,count",
		Step:      2 * 60 * 1000,
		From:      suite.basicQueryTime,
		To:        suite.basicQueryTime + int64(numberOfEvents*eventsInterval),
		GroupBy:   strings.Join(groupBy, ",")}
	set, err := querierV2.Select(params)
	suite.Require().NoError(err, "failed to exeute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		var groupByValue []string
		for _, label := range groupBy {
			groupByValue = append(groupByValue, set.At().Labels().Get(label))
		}
		labelsStr := strings.Join(groupByValue, "-")

		suite.Require().NoError(err)

		suite.Require().Equal(expected[labelsStr][agg], data, "queried data does not match expected")
	}

	suite.Require().Equal(6, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestGroupByNotExistingLabel() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux", "region", "europe")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "sum,count",
		Step:      2 * 60 * 1000,
		From:      suite.basicQueryTime,
		To:        suite.basicQueryTime + int64(numberOfEvents*eventsInterval),
		GroupBy:   "something that does not exist"}
	_, err = querierV2.Select(params)
	if err == nil {
		suite.T().Fatalf("expected fail but continued normally")
	}
}

func (suite *testQuerySuite) TestAggregatesWithZeroStep() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"max": {{Time: suite.basicQueryTime, Value: 40}},
		"min":   {{Time: suite.basicQueryTime, Value: 10}},
		"sum":   {{Time: suite.basicQueryTime, Value: 100}},
		"count": {{Time: suite.basicQueryTime, Value: 4}},
	}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "max, sum,count,min", Step: 0, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expected[agg], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 4, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestAggregatesWithZeroStepSelectDataframe() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string]tsdbtest.DataPoint{"max": {Time: suite.basicQueryTime, Value: 40},
		"min":   {Time: suite.basicQueryTime, Value: 10},
		"sum":   {Time: suite.basicQueryTime, Value: 100},
		"count": {Time: suite.basicQueryTime, Value: 4},
	}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "max, sum,count,min", Step: 0, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectDataFrame(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.NextFrame() {
		seriesCount++
		frame, err := set.GetFrame()
		suite.NoError(err)

		indexCol := frame.Indices()[0]
		assert.Equal(suite.T(), 1, indexCol.Len())
		t, err := indexCol.TimeAt(0)
		assert.NoError(suite.T(), err)
		assert.Equal(suite.T(), suite.basicQueryTime, t.UnixNano()/int64(time.Millisecond))

		for _, colName := range frame.Names() {
			col, err := frame.Column(colName)
			suite.NoError(err)
			suite.Require().Equal(1, col.Len())
			currentColAggregate := strings.Split(col.Name(), "(")[0]
			f, err := col.FloatAt(0)
			assert.NoError(suite.T(), err)
			suite.Require().Equal(expected[currentColAggregate].Value, f)
		}
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestEmptyRawDataSelectDataframe() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", From: suite.basicQueryTime - 10*tsdbtest.MinuteInMillis, To: suite.basicQueryTime - 1*tsdbtest.MinuteInMillis}
	set, err := querierV2.SelectDataFrame(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.NextFrame() {
		seriesCount++
		frame, err := set.GetFrame()
		suite.NoError(err)

		suite.Require().Equal(0, frame.Indices()[0].Len())

		for _, colName := range frame.Names() {
			col, _ := frame.Column(colName)
			assert.Equal(suite.T(), 0, col.Len())
		}
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) Test2Series1EmptySelectDataframe() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "diskio",
					Labels: labels1,
					Data:   []tsdbtest.DataPoint{{suite.basicQueryTime + 10*tsdbtest.MinuteInMillis, 10}}},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"cpu": ingestedData,
		"diskio": {{suite.basicQueryTime, math.NaN()},
			{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), math.NaN()},
			{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, math.NaN()},
			{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, math.NaN()}},
	}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params, _, _ := pquerier.ParseQuery("select cpu,diskio")
	params.From = suite.basicQueryTime
	params.To = suite.basicQueryTime + 4*tsdbtest.MinuteInMillis

	set, err := querierV2.SelectDataFrame(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.NextFrame() {
		seriesCount++
		frame, err := set.GetFrame()
		suite.NoError(err)

		indexCol := frame.Indices()[0]
		assert.Equal(suite.T(), len(ingestedData), indexCol.Len())
		for i := 0; i < indexCol.Len(); i++ {
			t, err := indexCol.TimeAt(i)
			assert.NoError(suite.T(), err)
			assert.Equal(suite.T(), ingestedData[i].Time, t.UnixNano()/int64(time.Millisecond))
		}

		for _, colName := range frame.Names() {
			col, err := frame.Column(colName)
			suite.NoError(err)
			assert.Equal(suite.T(), len(ingestedData), col.Len())
			for i := 0; i < col.Len(); i++ {
				currentExpected := expected[col.Name()][i].Value
				f, err := col.FloatAt(i)
				assert.NoError(suite.T(), err)

				if !(math.IsNaN(currentExpected) && math.IsNaN(f)) {
					assert.Equal(suite.T(), currentExpected, f)
				}
			}
		}
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestAggregateSeriesWithAlias() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedResult := 40.0

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	aliasName := "iguaz"
	params, _, _ := pquerier.ParseQuery(fmt.Sprintf("select max(cpu) as %v", aliasName))

	params.From = suite.basicQueryTime
	params.To = suite.basicQueryTime + int64(numberOfEvents*eventsInterval)

	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}
		assert.Equal(suite.T(), 1, len(data), "queried data does not match expected")
		assert.Equal(suite.T(), expectedResult, data[0].Value, "queried data does not match expected")

		seriesName := set.At().Labels().Get(config.PrometheusMetricNameAttribute)
		suite.Equal(aliasName, seriesName)
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestStringAndFloatMetricsDataframe() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.NoError(err, "failed to create v3io adapter")

	metricName1 := "cpu"
	metricName2 := "log"
	labels := utils.LabelsFromStringList("os", "linux")
	labelsWithName := append(labels, utils.LabelsFromStringList("__name__", metricName2)...)

	expectedTimeColumn := []int64{suite.basicQueryTime, suite.basicQueryTime + tsdbtest.MinuteInMillis, suite.basicQueryTime + 2*tsdbtest.MinuteInMillis}
	logData := []interface{}{"a", "b", "c"}
	expectedColumns := map[string][]interface{}{metricName1: {10.0, 20.0, 30.0},
		metricName2: logData}
	appender, err := adapter.Appender()
	suite.NoError(err, "failed to create v3io appender")

	ref, err := appender.Add(labelsWithName, expectedTimeColumn[0], logData[0])
	suite.NoError(err, "failed to add data to the TSDB appender")
	for i := 1; i < len(expectedTimeColumn); i++ {
		appender.AddFast(labels, ref, expectedTimeColumn[i], logData[i])
	}

	_, err = appender.WaitForCompletion(0)
	suite.NoError(err, "failed to wait for TSDB append completion")

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   metricName1,
				Labels: labels,
				Data: []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
					{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
					{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30}}},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	suite.NoError(err, "failed to create querier")

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: metricName1}, {Metric: metricName2}},
		From: suite.basicQueryTime, To: suite.basicQueryTime + 5*tsdbtest.MinuteInMillis}
	iter, err := querierV2.SelectDataFrame(params)
	suite.NoError(err, "failed to execute query")

	var seriesCount int
	for iter.NextFrame() {
		seriesCount++
		frame, err := iter.GetFrame()
		suite.NoError(err)
		indexCol := frame.Indices()[0]

		for i := 0; i < indexCol.Len(); i++ {
			t, _ := indexCol.TimeAt(i)
			timeMillis := t.UnixNano() / int64(time.Millisecond)
			suite.Require().Equal(expectedTimeColumn[i], timeMillis, "time column does not match at index %v", i)
			for _, columnName := range frame.Names() {
				var v interface{}

				column, err := frame.Column(columnName)
				suite.NoError(err)
				if column.DType() == frames.FloatType {
					v, _ = column.FloatAt(i)
				} else if column.DType() == frames.StringType {
					v, _ = column.StringAt(i)
				} else {
					suite.Fail(fmt.Sprintf("column type is not as expected: %v", column.DType()))
				}

				suite.Require().Equal(expectedColumns[column.Name()][i], v, "column %v does not match at index %v", column.Name(), i)
			}
		}
	}
}

func (suite *testQuerySuite) TestCrossSeriesAggregatesTimesFallsOnStep() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 20},
		{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 30}}
	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime, 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{
		"sum": {{Time: suite.basicQueryTime, Value: 30},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 50},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: suite.basicQueryTime, Value: 10},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 20},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 30}},
		"avg": {{Time: suite.basicQueryTime, Value: 15},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 25},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 35}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum_all,min_all,avg_all", Step: 2 * 60 * 1000, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	suite.Require().NoError(err, "Failed to execute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.Require().Equal(expected[agg], data, "queried data does not match expected")
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestCrossSeriesAggregates() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 20},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 30}}
	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime, 20},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{
		"sum": {{Time: suite.basicQueryTime, Value: 30},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 50},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: suite.basicQueryTime, Value: 10},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 20},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 30}},
		"avg": {{Time: suite.basicQueryTime, Value: 15},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 25},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 35}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum_all,min_all,avg_all", Step: 2 * 60 * 1000, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	suite.Require().NoError(err, "Failed to execute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.Require().Equal(expected[agg], data, "queried data does not match expected")
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestCrossSeriesAggregatesMultiPartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime, 20},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 60}}
	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 20},
		{suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime, 30},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{
		"max": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 20},
			{Time: suite.basicQueryTime, Value: 30},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 60}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Name: "cpu", Functions: "max_all", Step: 2 * 60 * 1000, From: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	suite.Require().NoError(err, "Failed to execute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.Require().Equal(expected[agg], data, "queried data does not match expected")
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestCrossSeriesAggregatesWithInterpolation() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 20},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, 40}}
	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime, 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{
		"sum": {{Time: suite.basicQueryTime, Value: 30},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 50},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: suite.basicQueryTime, Value: 10},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 20},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 30}},
		"max": {{Time: suite.basicQueryTime, Value: 20},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 30},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 40}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	selectParams, _, err := pquerier.ParseQuery("select sum_all(prev(cpu)), min_all(prev(cpu)), max_all(prev(cpu))")
	suite.NoError(err)
	selectParams.Step = 2 * tsdbtest.MinuteInMillis
	selectParams.From = suite.basicQueryTime
	selectParams.To = suite.basicQueryTime + 5*tsdbtest.MinuteInMillis
	set, err := querierV2.Select(selectParams)
	suite.Require().NoError(err, "Failed to execute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.Require().Equal(expected[agg], data, "queried data does not match expected")
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestCrossSeriesAggregatesMultiPartitionExactlyOnStep() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime, 20},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 60}}
	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 20},
		{suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime, 30},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{
		"sum": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 30},
			{Time: suite.basicQueryTime, Value: 50},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 100}},
		"min": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10},
			{Time: suite.basicQueryTime, Value: 20},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 40}},
		"avg": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 15},
			{Time: suite.basicQueryTime, Value: 25},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 50}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	selectParams, _, err := pquerier.ParseQuery("select sum_all(prev(cpu)), min_all(prev(cpu)),avg_all(prev(cpu))")
	suite.NoError(err)
	selectParams.Step = 2 * tsdbtest.MinuteInMillis
	selectParams.From = suite.basicQueryTime - 7*tsdbtest.DaysInMillis
	selectParams.To = suite.basicQueryTime + 5*tsdbtest.MinuteInMillis
	set, err := querierV2.Select(selectParams)
	suite.Require().NoError(err, "Failed to execute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.Require().Equal(expected[agg], data, "queried data does not match expected")
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestCrossSeriesAggregatesMultiPartitionWithInterpolation() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 3*tsdbtest.MinuteInMillis, 20},
		{suite.basicQueryTime, 20},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 60}}
	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 20},
		{suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 2*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime, 30},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{
		"sum": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 30},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 2*tsdbtest.MinuteInMillis, Value: 2},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 4*tsdbtest.MinuteInMillis, Value: 21},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 6*tsdbtest.MinuteInMillis, Value: 21},
			{Time: suite.basicQueryTime, Value: 50},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 61}},
		"count": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 2},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 2*tsdbtest.MinuteInMillis, Value: 2},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 4*tsdbtest.MinuteInMillis, Value: 2},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 6*tsdbtest.MinuteInMillis, Value: 2},
			{Time: suite.basicQueryTime, Value: 2},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 2}},
		"min": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 2*tsdbtest.MinuteInMillis, Value: 1},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 4*tsdbtest.MinuteInMillis, Value: 1},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 6*tsdbtest.MinuteInMillis, Value: 1},
			{Time: suite.basicQueryTime, Value: 20},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 1}},
		"avg": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 15},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 2*tsdbtest.MinuteInMillis, Value: 1},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 4*tsdbtest.MinuteInMillis, Value: 10.5},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 6*tsdbtest.MinuteInMillis, Value: 10.5},
			{Time: suite.basicQueryTime, Value: 25},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 30.5}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	selectParams, _, err := pquerier.ParseQuery("select sum_all(prev(cpu)), min_all(prev(cpu)),avg_all(prev(cpu)),count_all(prev(cpu))")
	suite.NoError(err)
	selectParams.Step = 2 * tsdbtest.MinuteInMillis
	selectParams.From = suite.basicQueryTime - 7*tsdbtest.DaysInMillis
	selectParams.To = suite.basicQueryTime + 5*tsdbtest.MinuteInMillis
	set, err := querierV2.Select(selectParams)
	suite.Require().NoError(err, "Failed to execute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.Require().Equal(expected[agg], data, "queried data does not match expected")
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestCrossSeriesAggregatesWithInterpolationOverTolerance() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 20},
		{suite.basicQueryTime + 10*tsdbtest.MinuteInMillis, 30}}
	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime, 20},
		{suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 10*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels2,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{
		"sum": {{Time: suite.basicQueryTime, Value: 30},
			{Time: suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, Value: 30},
			{Time: suite.basicQueryTime + 10*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: suite.basicQueryTime, Value: 10},
			{Time: suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, Value: 30},
			{Time: suite.basicQueryTime + 10*tsdbtest.MinuteInMillis, Value: 30}},
		"max": {{Time: suite.basicQueryTime, Value: 20},
			{Time: suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, Value: 30},
			{Time: suite.basicQueryTime + 10*tsdbtest.MinuteInMillis, Value: 40}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	selectParams, _, err := pquerier.ParseQuery("select sum_all(prev(cpu)), min_all(prev(cpu)), max_all(prev(cpu))")
	suite.NoError(err)
	selectParams.Step = 5 * tsdbtest.MinuteInMillis
	selectParams.From = suite.basicQueryTime
	selectParams.To = suite.basicQueryTime + 10*tsdbtest.MinuteInMillis
	for i := 0; i < len(selectParams.RequestedColumns); i++ {
		selectParams.RequestedColumns[i].InterpolationTolerance = tsdbtest.MinuteInMillis
	}
	set, err := querierV2.Select(selectParams)
	suite.Require().NoError(err, "Failed to execute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.Require().Equal(expected[agg], data, "queried data does not match expected")
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestQueryMultipleMetricsWithMultipleLabelSets() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	metricName1 := "cpu"
	metricName2 := "diskio"
	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	numberOfEvents := 5
	eventsInterval := int64(tsdbtest.MinuteInMillis)
	ingestData1 := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	ingestData2 := []tsdbtest.DataPoint{{suite.basicQueryTime, 20}}
	ingestData3 := []tsdbtest.DataPoint{{suite.basicQueryTime, 30},
		{suite.basicQueryTime + tsdbtest.MinuteInMillis, 40}}

	expectedData := map[string][]tsdbtest.DataPoint{fmt.Sprintf("%v-%v", metricName1, "linux"): ingestData1,
		fmt.Sprintf("%v-%v", metricName2, "linux"): ingestData2,
		fmt.Sprintf("%v-%v", metricName2, "mac"):   ingestData3}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   metricName1,
				Labels: labels1,
				Data:   ingestData1},
				tsdbtest.Metric{
					Name:   metricName2,
					Labels: labels1,
					Data:   ingestData2},
				tsdbtest.Metric{
					Name:   metricName2,
					Labels: labels2,
					Data:   ingestData3},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Filter: "1==1",
		From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents)*eventsInterval}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		name := set.At().Labels().Get(config.PrometheusMetricNameAttribute)
		os := set.At().Labels().Get("os")
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expectedData[fmt.Sprintf("%v-%v", name, os)], data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) TestQueryDataFrameMultipleMetricsWithMultipleLabelSets() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	metricName1 := "cpu"
	metricName2 := "diskio"
	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	numberOfEvents := 5
	eventsInterval := int64(tsdbtest.MinuteInMillis)
	ingestData1 := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	ingestData2 := []tsdbtest.DataPoint{{suite.basicQueryTime + tsdbtest.MinuteInMillis, 20}}
	ingestData3 := []tsdbtest.DataPoint{{suite.basicQueryTime, 30},
		{suite.basicQueryTime + tsdbtest.MinuteInMillis, 40}}

	expectedData := map[string][]tsdbtest.DataPoint{
		fmt.Sprintf("%v-%v", metricName1, "linux"): {{suite.basicQueryTime, 10}, {suite.basicQueryTime + tsdbtest.MinuteInMillis, math.NaN()}},
		fmt.Sprintf("%v-%v", metricName2, "linux"): {{suite.basicQueryTime, math.NaN()}, {suite.basicQueryTime + tsdbtest.MinuteInMillis, 20}},
		fmt.Sprintf("%v-%v", metricName2, "mac"):   ingestData3}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   metricName1,
				Labels: labels1,
				Data:   ingestData1},
				tsdbtest.Metric{
					Name:   metricName2,
					Labels: labels1,
					Data:   ingestData2},
				tsdbtest.Metric{
					Name:   metricName2,
					Labels: labels2,
					Data:   ingestData3},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Filter: "1==1",
		From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents)*eventsInterval}
	set, err := querierV2.SelectDataFrame(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.NextFrame() {
		seriesCount++
		frame, err := set.GetFrame()
		suite.NoError(err)

		indexCol := frame.Indices()[0]
		assert.Equal(suite.T(), 2, indexCol.Len())
		for i := 0; i < indexCol.Len(); i++ {
			t, err := indexCol.TimeAt(i)
			assert.NoError(suite.T(), err)
			assert.Equal(suite.T(), expectedData[fmt.Sprintf("%v-%v", metricName1, "linux")][i].Time, t.UnixNano()/int64(time.Millisecond))

			for _, colName := range frame.Names() {
				col, err := frame.Column(colName)
				suite.NoError(err)
				currentExpectedData := expectedData[fmt.Sprintf("%v-%v", col.Name(), frame.Labels()["os"])]
				assert.Equal(suite.T(), len(currentExpectedData), col.Len())
				currentExpected := currentExpectedData[i].Value
				f, err := col.FloatAt(i)
				assert.NoError(suite.T(), err)

				if !(math.IsNaN(currentExpected) && math.IsNaN(f)) {
					assert.Equal(suite.T(), currentExpected, f)
				}
			}
		}
	}

	assert.Equal(suite.T(), 2, seriesCount, "series count didn't match expected")
}

func (suite *testQuerySuite) toMillis(date string) int64 {
	time, err := tsdbtest.DateStringToMillis(date)
	suite.NoError(err)
	return time
}

func TestQueryV2Suite(t *testing.T) {
	suite.Run(t, new(testQuerySuite))
}

type getLabelSetsSuite struct {
	suite.Suite
	v3ioConfig     *config.V3ioConfig
	suiteTimestamp int64
	basicQueryTime int64
}

func (suite *getLabelSetsSuite) SetupSuite() {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		suite.T().Fatalf("unable to load configuration. Error: %v", err)
	}

	suite.v3ioConfig = v3ioConfig
	suite.suiteTimestamp = time.Now().Unix()
	suite.basicQueryTime, err = tsdbtest.DateStringToMillis("2018-07-21T10:00:00Z")
	suite.NoError(err)
}

func (suite *getLabelSetsSuite) SetupTest() {
	suite.v3ioConfig.TablePath = fmt.Sprintf("%s-%v", suite.T().Name(), suite.suiteTimestamp)
	tsdbtest.CreateTestTSDB(suite.T(), suite.v3ioConfig)
}

func (suite *getLabelSetsSuite) TearDownTest() {
	suite.v3ioConfig.TablePath = fmt.Sprintf("%s-%v", suite.T().Name(), suite.suiteTimestamp)
	if !suite.T().Failed() {
		tsdbtest.DeleteTSDB(suite.T(), suite.v3ioConfig)
	}
}

func (suite *getLabelSetsSuite) TestGetLabels() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe"),
		utils.LabelsFromStringList("os", "linux", "region", "asia"),
		utils.LabelsFromStringList("os", "mac", "region", "europe")}
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels[0],
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[1],
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[2],
					Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedLabels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "linux", "region", "asia", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "mac", "region", "europe", config.PrometheusMetricNameAttribute, "cpu")}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	labelsList, err := querierV2.GetLabelSets("cpu", "")
	if err != nil {
		suite.T().Fatalf("failed to get label sets, err:%v\n", err)
	}

	suite.ElementsMatch(expectedLabels, labelsList, "actual label sets does not match expected")
}

func (suite *getLabelSetsSuite) TestGetLabelsAllMetrics() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe"),
		utils.LabelsFromStringList("os", "linux", "region", "asia"),
		utils.LabelsFromStringList("os", "mac", "region", "europe")}
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels[0],
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[1],
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "diskio",
					Labels: labels[2],
					Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedLabels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "linux", "region", "asia", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "mac", "region", "europe", config.PrometheusMetricNameAttribute, "diskio")}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	labelsList, err := querierV2.GetLabelSets("", "")
	if err != nil {
		suite.T().Fatalf("failed to get label sets, err:%v\n", err)
	}

	suite.ElementsMatch(expectedLabels, labelsList, "actual label sets does not match expected")
}

func (suite *getLabelSetsSuite) TestGetLabelsAllSpecificMetric() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe"),
		utils.LabelsFromStringList("os", "linux", "region", "asia"),
		utils.LabelsFromStringList("os", "mac", "region", "europe")}
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels[0],
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[1],
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "diskio",
					Labels: labels[2],
					Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedLabels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "linux", "region", "asia", config.PrometheusMetricNameAttribute, "cpu")}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	labelsList, err := querierV2.GetLabelSets("cpu", "")
	if err != nil {
		suite.T().Fatalf("failed to get label sets, err:%v\n", err)
	}

	suite.ElementsMatch(expectedLabels, labelsList, "actual label sets does not match expected")
}

func (suite *getLabelSetsSuite) TestGetLabelsWithFilter() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe"),
		utils.LabelsFromStringList("os", "linux", "region", "asia"),
		utils.LabelsFromStringList("os", "mac", "region", "europe")}
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels[0],
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[1],
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[2],
					Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedLabels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "linux", "region", "asia", config.PrometheusMetricNameAttribute, "cpu")}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	labelsList, err := querierV2.GetLabelSets("cpu", "os=='linux'")
	if err != nil {
		suite.T().Fatalf("failed to get label sets, err:%v\n", err)
	}

	suite.ElementsMatch(expectedLabels, labelsList, "actual label sets does not match expected")
}

func TestGetLabelSetsSuite(t *testing.T) {
	suite.Run(t, new(getLabelSetsSuite))
}
