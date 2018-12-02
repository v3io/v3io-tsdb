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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type testQuerySuite struct {
	suite.Suite
	v3ioConfig *config.V3ioConfig
}

func (suite *testQuerySuite) SetupSuite() {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		suite.T().Fatalf("unable to load configuration. Error: %v", err)
	}

	suite.v3ioConfig = v3ioConfig
}

func (suite *testQuerySuite) SetupTest() {
	suite.v3ioConfig.TablePath = suite.T().Name()
	tsdbtest.CreateTestTSDB(suite.T(), suite.v3ioConfig)
}

func (suite *testQuerySuite) TearDownTest() {
	suite.v3ioConfig.TablePath = suite.T().Name()
	tsdbtest.DeleteTSDB(suite.T(), suite.v3ioConfig)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)
	expectedData := []tsdbtest.DataPoint{{baseTime, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*tsdbtest.MinuteInMillis, 30},
		{baseTime + 3*tsdbtest.MinuteInMillis, 40}}
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

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents)*eventsInterval
	expectedData := []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*eventsInterval, 30},
		{baseTime + 3*eventsInterval, 40}}

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

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", From: baseTime - 8*tsdbtest.DaysInMillis, To: baseTime + int64(numberOfEvents)*eventsInterval}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents)*eventsInterval
	expectedData := []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*eventsInterval, 30},
		{baseTime + 3*eventsInterval, 40}}

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

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Filter: "os=='linux'",
		From: baseTime - 8*tsdbtest.DaysInMillis, To: baseTime + int64(numberOfEvents)*eventsInterval}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)
	expectedData := []tsdbtest.DataPoint{{baseTime, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*tsdbtest.MinuteInMillis, 30},
		{baseTime + 3*tsdbtest.MinuteInMillis, 40}}
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

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", From: baseTime + int64(numberOfEvents*eventsInterval), To: baseTime}
	_, err = querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)
	expectedData := []tsdbtest.DataPoint{{baseTime, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*tsdbtest.MinuteInMillis, 30},
		{baseTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cool-cpu",
				Labels: labels1,
				Data:   expectedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}
	_, err = querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)
	ingestData := []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*tsdbtest.MinuteInMillis, 30},
		{baseTime + 3*tsdbtest.MinuteInMillis, 40}}
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
		"sum": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: baseTime, Value: 20}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: baseTime, Value: 20}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 30}},
		"max": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: baseTime, Value: 20}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 40}}}
	expected := map[string]map[string][]tsdbtest.DataPoint{"cpu": expectedData, "diskio": expectedData}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Functions: "max,min,sum", Step: 2 * tsdbtest.MinuteInMillis,
		From: baseTime - 7*tsdbtest.DaysInMillis, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)
	ingestData := []tsdbtest.DataPoint{{baseTime, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*tsdbtest.MinuteInMillis, 30},
		{baseTime + 3*tsdbtest.MinuteInMillis, 40}}
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
	expectedData := map[string][]tsdbtest.DataPoint{"max": {{Time: baseTime, Value: 20}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 40}}}
	expected := map[string]map[string][]tsdbtest.DataPoint{"cpu": expectedData}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Functions: "max", Step: 2 * tsdbtest.MinuteInMillis,
		From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval), Filter: "_name=='cpu'"}
	set, err := querierV2.SelectQry(params)
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

func (suite *testQuerySuite) TestRawDataSinglePartitionWithDownSample() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)
	ingestData := []tsdbtest.DataPoint{{baseTime, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 6*tsdbtest.MinuteInMillis, 30},
		{baseTime + 9*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expectedData := []tsdbtest.DataPoint{{baseTime, 10},
		{baseTime + 6*tsdbtest.MinuteInMillis, 30},
		{baseTime + 8*tsdbtest.MinuteInMillis, 40}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Step: 2 * int64(tsdbtest.MinuteInMillis), From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu"}},
		Step: 2 * int64(tsdbtest.HoursInMillis),
		From: suite.toMillis("2018-11-18T22:00:00Z"),
		To:   suite.toMillis("2018-11-19T4:00:00Z")}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

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

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min", Step: 1 * 60 * 60 * 1000, From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

	ingestedData := []tsdbtest.DataPoint{{baseTime, -10},
		{int64(baseTime + tsdbtest.MinuteInMillis), -20},
		{baseTime + 2*tsdbtest.MinuteInMillis, -30},
		{baseTime + 3*tsdbtest.MinuteInMillis, -40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: baseTime, Value: -100}},
		"min": {{Time: baseTime, Value: -40}},
		"max": {{Time: baseTime, Value: -10}}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min", Step: 1 * 60 * 60 * 1000, From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

	ingestedData := []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
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

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: baseTime, Value: 90}},
		"min": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: baseTime, Value: 20}},
		"max": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: baseTime, Value: 40}},
		"sqr": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 100}, {Time: baseTime, Value: 2900}}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min,sqr", Step: 1 * 60 * 60 * 1000, From: baseTime - 7*tsdbtest.DaysInMillis, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

	ingestedData := []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
		{baseTime - 7*tsdbtest.DaysInMillis + tsdbtest.MinuteInMillis, 12},
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

	expected := map[string][]tsdbtest.DataPoint{"avg": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 11}, {Time: baseTime, Value: 30}},
		"stdvar": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 2}, {Time: baseTime, Value: 100}}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "avg,stdvar", Step: 1 * 60 * 60 * 1000, From: baseTime - 7*tsdbtest.DaysInMillis, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

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

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: baseTime, Value: 30}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: baseTime, Value: 10}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 30}},
		"max": {{Time: baseTime, Value: 20}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 40}}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min", Step: 2 * 60 * 1000, From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

	ingestedData := []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(baseTime), 20},
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

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: baseTime, Value: 90}},
		"min": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: baseTime, Value: 20}},
		"max": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 10}, {Time: baseTime, Value: 40}}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "sum,max,min",
		Step:      5 * tsdbtest.MinuteInMillis,
		From:      baseTime - 7*tsdbtest.DaysInMillis,
		To:        baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

	ingestedData := []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
		{baseTime - 7*tsdbtest.DaysInMillis + tsdbtest.MinuteInMillis, 12},
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

	expected := map[string][]tsdbtest.DataPoint{"avg": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 11}, {Time: baseTime, Value: 30}},
		"stdvar": {{Time: baseTime - 7*tsdbtest.DaysInMillis, Value: 2}, {Time: baseTime, Value: 100}}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "avg,stdvar",
		Step:      5 * tsdbtest.MinuteInMillis,
		From:      baseTime - 7*tsdbtest.DaysInMillis,
		To:        baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

	ingestedData := []tsdbtest.DataPoint{{baseTime - 25*tsdbtest.DaysInMillis, 10},
		{baseTime - 20*tsdbtest.DaysInMillis, 20},
		{baseTime - 12*tsdbtest.DaysInMillis, 30},
		{baseTime - 1*tsdbtest.DaysInMillis, 40},
		{baseTime + 20*tsdbtest.DaysInMillis, 50}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"count": {{Time: baseTime - 25*tsdbtest.DaysInMillis, Value: 5}}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "count",
		Step:      0,
		From:      baseTime - 25*tsdbtest.DaysInMillis,
		To:        baseTime + 21*tsdbtest.DaysInMillis}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

	ingestedData := []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
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

	expected := map[string][]tsdbtest.DataPoint{}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "i dont exist", Functions: "sum,max,min,sqr", Step: 1 * 60 * 60 * 1000, From: baseTime - 7*tsdbtest.DaysInMillis, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

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

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: baseTime, Value: 30}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 70}},
		"min": {{Time: baseTime, Value: 10}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 30}},
		"max": {{Time: baseTime, Value: 20}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 40}}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu", Function: "max"}, {Metric: "cpu", Function: "min"}, {Metric: "cpu", Function: "sum"}},
		Step: 2 * 60 * 1000, From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}

	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

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

	expected := ingestedData

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu"}},
		From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

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

	expected := map[string][]tsdbtest.DataPoint{"sum": {{Time: baseTime, Value: 30}, {Time: baseTime + 2*tsdbtest.MinuteInMillis, Value: 70}},
		"": {{baseTime, 10}, {baseTime + 2*tsdbtest.MinuteInMillis, 30}}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu", Function: "sum"}, {Metric: "cpu"}},
		Step: 2 * 60 * 1000, From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}

	set, err := querierV2.SelectQry(params)
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
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents*eventsInterval)

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
		"": {{baseTime, 10}}}

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu", Function: "sum"}, {Metric: "cpu", Interpolator: "next"}},
		Step: 60 * tsdbtest.MinuteInMillis, From: baseTime, To: baseTime + int64(numberOfEvents*eventsInterval)}

	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents)*eventsInterval
	ingestData1 := []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*eventsInterval, 30},
		{baseTime + 4*eventsInterval, 40}}
	ingestData2 := []tsdbtest.DataPoint{{baseTime - 5*tsdbtest.DaysInMillis, 10},
		{int64(baseTime + 2*tsdbtest.MinuteInMillis), 20},
		{baseTime + 3*eventsInterval, 30},
		{baseTime + 4*eventsInterval, 40}}

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

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: metricName1}, {Metric: metricName2}},
		From: baseTime - 8*tsdbtest.DaysInMillis, To: baseTime + int64(numberOfEvents)*eventsInterval}
	set, err := querierV2.SelectQry(params)
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
	baseTime := tsdbtest.NanosToMillis(time.Now().UnixNano()) - int64(numberOfEvents)*eventsInterval
	expectedTimeColumn := []int64{baseTime - 7*tsdbtest.DaysInMillis, baseTime - 5*tsdbtest.DaysInMillis,
		baseTime + tsdbtest.MinuteInMillis, baseTime + 2*tsdbtest.MinuteInMillis,
		baseTime + 3*tsdbtest.MinuteInMillis, baseTime + 4*tsdbtest.MinuteInMillis}
	expectedColumns := map[string][]float64{metricName1: {10, math.NaN(), 20, 30, math.NaN(), 40},
		metricName2: {math.NaN(), 10, math.NaN(), 20, 30, 40}}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   metricName1,
				Labels: labels1,
				Data: []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
					{int64(baseTime + tsdbtest.MinuteInMillis), 20},
					{baseTime + 2*tsdbtest.MinuteInMillis, 30},
					{baseTime + 4*tsdbtest.MinuteInMillis, 40}}},
				tsdbtest.Metric{
					Name:   metricName2,
					Labels: labels2,
					Data: []tsdbtest.DataPoint{{baseTime - 5*tsdbtest.DaysInMillis, 10},
						{int64(baseTime + 2*tsdbtest.MinuteInMillis), 20},
						{baseTime + 3*tsdbtest.MinuteInMillis, 30},
						{baseTime + 4*tsdbtest.MinuteInMillis, 40}}},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2(nil)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: metricName1}, {Metric: metricName2}},
		From: baseTime - 8*tsdbtest.DaysInMillis, To: baseTime + int64(numberOfEvents)*eventsInterval}
	iter, err := querierV2.SelectDataFrame(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}
	var seriesCount int
	for iter.NextFrame() {
		seriesCount++
		frame := iter.GetFrame()
		in := frame.Index()
		cols := frame.Columns()

		for i := 0; i < frame.Index().Len(); i++ {
			t, _ := in.TimeAt(i)
			assert.Equal(suite.T(), expectedTimeColumn[i], t, "time column does not match at index %v", i)
			for _, column := range cols {
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

func (suite *testQuerySuite) toMillis(date string) int64 {
	t, err := time.Parse(time.RFC3339, date)
	if err != nil {
		suite.T().Fatal(err)
	}
	return t.Unix() * 1000
}

func TestQueryV2Suite(t *testing.T) {
	suite.Run(t, new(testQuerySuite))
}
