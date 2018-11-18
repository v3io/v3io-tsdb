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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"testing"
	"time"
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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	labels2 := utils.LabelsFromStrings("__name__", metricName, "os", "mac")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)
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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	labels2 := utils.LabelsFromStrings("__name__", metricName, "os", "mac")
	numberOfEvents := 5
	eventsInterval := int64(tsdbtest.MinuteInMillis)
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents)*eventsInterval
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

func (suite *testQuerySuite) TestRawDataSinglePartitionWithDownSample() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)
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

func (suite *testQuerySuite) TestRawAggregatesSinglePartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")

	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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

func (suite *testQuerySuite) TestGetEmptyResponse() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)

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

func TestQueryV2Suite(t *testing.T) {
	suite.Run(t, new(testQuerySuite))
}

const defaultStepMs = 5 * tsdbtest.MinuteInMillis // 5 minutes
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
						Labels: utils.LabelsFromStrings("testLabel", "balbala"),
						Data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}},
					}}},
			),
			from:     0,
			to:       1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 314.3}}}},

		{desc: "Should ingest and query multiple data points",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510 - 10, Value: 314.3},
							{Time: 1532940510 - 5, Value: 300.3},
							{Time: 1532940510, Value: 3234.6}},
					}}},
			),
			from: 0,
			to:   1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510 - 10, Value: 314.3},
				{Time: 1532940510 - 5, Value: 300.3},
				{Time: 1532940510, Value: 3234.6}}}},

		{desc: "Should query with filter on metric name",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
						Data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 33.3}},
					}}},
			),
			filter:   "_name=='cpu'",
			from:     0,
			to:       1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 33.3}}}},

		{desc: "Should query with filter on label name",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
						Data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 31.3}},
					}}},
			),
			filter:   "os=='linux'",
			from:     0,
			to:       1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 31.3}}}},

		{desc: "Should ingest and query data with '-' in the metric name (IG-8585)",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cool-cpu",
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
						Data:   []tsdbtest.DataPoint{{Time: 1532940510, Value: 314.3}},
					}}},
			),
			from:     0,
			to:       1532940510 + 1,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510, Value: 314.3}}}},

		{desc: "Should ingest and query by time",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 314.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 + 10, Value: 3234.6}},
					}}},
			),
			from: 1532940510 + 2,
			to:   1532940510 + 12,
			expected: map[string][]tsdbtest.DataPoint{"": {{Time: 1532940510 + 5, Value: 300.3},
				{Time: 1532940510 + 10, Value: 3234.6}}}},

		{desc: "Should ingest and query by time with no results",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
						Data: []tsdbtest.DataPoint{
							{Time: 1532940510, Value: 314.3},
							{Time: 1532940510 + 5, Value: 300.3},
							{Time: 1532940510 + 10, Value: 3234.6}},
					}}},
			),
			from:     1532940510 + 1,
			to:       1532940510 + 4,
			expected: map[string][]tsdbtest.DataPoint{}},

		{desc: "Should ingest and query an aggregate",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
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
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
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
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
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
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
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
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
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
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
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
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
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
				"count": {{Time: 1537972278402, Value: 1},
					{Time: 1537972278402 + 15*tsdbtest.MinuteInMillis, Value: 2}}}},

		{desc: "Should ingest and query server-side aggregates",
			testParams: tsdbtest.NewTestParams(t,
				tsdbtest.TestOption{
					Key: tsdbtest.OptTimeSeries,
					Value: tsdbtest.TimeSeries{tsdbtest.Metric{
						Name:   "cpu",
						Labels: utils.LabelsFromStrings("os", "linux", "iguaz", "yesplease"),
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
func testQueryDataCase(test *testing.T, params tsdbtest.TestParams, filter string, agg string,
	from int64, to int64, step int64, expected map[string][]tsdbtest.DataPoint, expectFail bool) {
	adapter, teardown := tsdbtest.SetUpWithData(test, params)
	defer teardown()

	qry, err := adapter.QuerierV2(nil)
	if err != nil {
		test.Fatalf("Failed to create Querier. reason: %v", err)
	}

	for _, metric := range params.TimeSeries() {
		selectParams := &pquerier.SelectParams{Name: metric.Name, Functions: agg, Step: step, Filter: filter, From: from, To: to}
		set, err := qry.SelectQry(selectParams)
		if err != nil {
			if expectFail {
				return
			} else {
				test.Fatalf("Failed to run Select. reason: %v", err)
			}
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

			fmt.Println(test.Name(), "actual:", actual, "expected: ", expected[agg])
			assert.ElementsMatch(test, expected[agg], actual)
		}

		if set.Err() != nil {
			test.Fatalf("Failed to query metric. reason: %v", set.Err())
		}
		if counter == 0 && len(expected) > 0 {
			test.Fatalf("No data was received")
		}
	}
}
