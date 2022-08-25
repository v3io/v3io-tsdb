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

package pqueriertest

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type testClientAggregatesSuite struct {
	basicQueryTestSuite
}

func TestClientAggregatesSuite(t *testing.T) {
	suite.Run(t, new(testClientAggregatesSuite))
}

func (suite *testClientAggregatesSuite) TestQueryAggregateWithNameWildcard() {
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

		suite.compareMultipleMetrics(data, expected, metricName, aggr)
	}

	assert.Equal(suite.T(), len(expectedData)*len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestQueryAggregateWithFilterOnMetricName() {
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

		suite.compareMultipleMetrics(data, expected, metricName, aggr)
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestClientAggregatesSinglePartition() {
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestClientAggregatesMultiPartition() {
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
				Labels: labels1,
				Name:   "cpu",
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestClientAggregatesMultiPartitionNonConcreteAggregates() {
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	assert.Equal(suite.T(), len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestClientAggregatesMultiPartitionOneStep() {
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestGetEmptyResponse() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")

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

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min,sqr",
		Step: 1 * 60 * 60 * 1000,
		From: suite.basicQueryTime - 10*tsdbtest.DaysInMillis,
		To:   suite.basicQueryTime - 8*tsdbtest.DaysInMillis}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
	}

	assert.Equal(suite.T(), 0, seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestSelectAggregatesByRequestedColumns() {
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestSelectAggregatesAndRawByRequestedColumns() {
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	assert.Equal(suite.T(), 2, seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestQueryAllData() {
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	assert.Equal(suite.T(), 3, seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestAggregatesWithZeroStep() {
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

	expected := map[string][]tsdbtest.DataPoint{
		"max":   {{Time: suite.basicQueryTime, Value: 40}},
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

		for i, dataPoint := range expected[agg] {
			suite.Require().True(dataPoint.Equals(data[i]), "queried data does not match expected")
		}
	}

	assert.Equal(suite.T(), 4, seriesCount, "series count didn't match expected")
}

func (suite *testClientAggregatesSuite) TestUsePreciseAggregationsConfig() {
	suite.v3ioConfig.UsePreciseAggregations = true
	defer func() { suite.v3ioConfig.UsePreciseAggregations = false }()
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.NoError(err, "failed to create v3io adapter.")

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
	suite.NoError(err, "failed to create querier v2.")

	params := &pquerier.SelectParams{Name: "cpu", Functions: "sum,max,min", Step: 1 * 60 * 60 * 1000, From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	suite.NoError(err, "failed to exeute query,")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	suite.Require().Equal(3, seriesCount, "series count didn't match expected")
}
