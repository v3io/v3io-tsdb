// +build integration

package pqueriertest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type testCrossSeriesAggregatesSuite struct {
	basicQueryTestSuite
}

func TestCrossSeriesAggregatesSuite(t *testing.T) {
	suite.Run(t, new(testCrossSeriesAggregatesSuite))
}

func (suite *testCrossSeriesAggregatesSuite) TestCrossSeriesAggregatesTimesFallsOnStep() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")

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

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "sum_all,min_all,avg_all",
		Step:      2 * 60 * 1000,
		From:      suite.basicQueryTime,
		To:        suite.basicQueryTime + 5*tsdbtest.MinuteInMillis}
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
		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) TestCrossSeriesAggregates() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")

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

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "sum_all,min_all,avg_all",
		Step:      2 * 60 * 1000,
		From:      suite.basicQueryTime,
		To:        suite.basicQueryTime + 5*tsdbtest.MinuteInMillis}
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) TestCrossSeriesAggregatesMultiPartition() {
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
		"max": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 20},
			{Time: suite.basicQueryTime - 4*tsdbtest.MinuteInMillis, Value: 30},
			{Time: suite.basicQueryTime - 2*tsdbtest.MinuteInMillis, Value: 30},
			{Time: suite.basicQueryTime, Value: 30},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 60}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "max_all",
		Step:      2 * 60 * 1000,
		From:      suite.basicQueryTime - 7*tsdbtest.DaysInMillis,
		To:        suite.basicQueryTime + 3*tsdbtest.MinuteInMillis}
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) TestCrossSeriesAggregatesWithInterpolation() {
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

	selectParams, _, err := pquerier.ParseQuery("select sum_all(prev_val(cpu)), min_all(prev_val(cpu)), max_all(prev_val(cpu))")
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) TestCrossSeriesAggregatesMultiPartitionExactlyOnStep() {
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
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 2*tsdbtest.MinuteInMillis, Value: 2},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 4*tsdbtest.MinuteInMillis, Value: 2},
			{Time: suite.basicQueryTime, Value: 50},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 100}},
		"min": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 10},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 2*tsdbtest.MinuteInMillis, Value: 1},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 4*tsdbtest.MinuteInMillis, Value: 1},
			{Time: suite.basicQueryTime, Value: 20},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 40}},
		"avg": {{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis, Value: 15},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 2*tsdbtest.MinuteInMillis, Value: 1},
			{Time: suite.basicQueryTime - 7*tsdbtest.DaysInMillis + 4*tsdbtest.MinuteInMillis, Value: 1},
			{Time: suite.basicQueryTime, Value: 25},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 50}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	selectParams, _, err := pquerier.ParseQuery("select sum_all(prev_val(cpu)), min_all(prev_val(cpu)),avg_all(prev_val(cpu))")
	suite.NoError(err)
	selectParams.Step = 2 * tsdbtest.MinuteInMillis
	selectParams.From = suite.basicQueryTime - 7*tsdbtest.DaysInMillis
	selectParams.To = suite.basicQueryTime + 3*tsdbtest.MinuteInMillis
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
		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) TestCrossSeriesAggregatesMultiPartitionWithInterpolation() {
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

	selectParams, _, err := pquerier.ParseQuery("select sum_all(prev_val(cpu)), min_all(prev_val(cpu)),avg_all(prev_val(cpu)),count_all(prev_val(cpu))")
	suite.NoError(err)
	selectParams.Step = 2 * tsdbtest.MinuteInMillis
	selectParams.From = suite.basicQueryTime - 7*tsdbtest.DaysInMillis
	selectParams.To = suite.basicQueryTime + 3*tsdbtest.MinuteInMillis
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
		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) TestCrossSeriesAggregatesWithInterpolationOverTolerance() {
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

	selectParams, _, err := pquerier.ParseQuery("select sum_all(prev_val(cpu)), min_all(prev_val(cpu)), max_all(prev_val(cpu))")
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
		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) TestCrossSeriesAggregatesSinglePartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")

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

	params := &pquerier.SelectParams{Name: "cpu",
		Functions: "sum_all,min_all,max_all,count_all,avg_all",
		Step:      2 * 60 * 1000,
		From:      suite.basicQueryTime,
		To:        suite.basicQueryTime + 1*tsdbtest.MinuteInMillis}
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

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) TestOnlyVirtualCrossSeriesAggregateWithInterpolation() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "mac")
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 20},
		{suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, 20}}
	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime, 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 1},
		{suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, 20}}
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
		"avg": {{Time: suite.basicQueryTime, Value: 15},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 1},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 10.5}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	selectParams, _, err := pquerier.ParseQuery("select avg_all(prev_val(cpu))")
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
		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) TestCrossSeriesAggregatesSameLabelMultipleMetrics() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")

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
					Name:   "disk",
					Labels: labels1,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{
		"sum-cpu": {{Time: suite.basicQueryTime, Value: 10},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 20},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 30}},
		"sum-disk": {{Time: suite.basicQueryTime, Value: 20},
			{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 30},
			{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 40}}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Name: "cpu, disk",
		Functions: "sum_all",
		Step:      2 * 60 * 1000,
		From:      suite.basicQueryTime,
		To:        suite.basicQueryTime + 5*tsdbtest.MinuteInMillis}
	set, err := querierV2.Select(params)
	suite.Require().NoError(err, "Failed to execute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		suite.NoError(err)

		agg := set.At().Labels().Get(aggregate.AggregateLabel)
		suite.NoError(err)

		metricName := set.At().Labels().Get(config.PrometheusMetricNameAttribute)
		suite.NoError(err)

		suite.compareSingleMetricWithAggregator(data, expected, fmt.Sprintf("%v-%v", agg, metricName))
	}

	suite.Require().Equal(len(expected), seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) TestCrossSeriesAggregatesDifferentLabelMultipleMetrics() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels1 := utils.LabelsFromStringList("os", "linux")
	labels2 := utils.LabelsFromStringList("os", "darwin")

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
				tsdbtest.Metric{
					Name:   "disk",
					Labels: labels1,
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "disk",
					Labels: labels2,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := []tsdbtest.DataPoint{
		{Time: suite.basicQueryTime, Value: 30},
		{Time: suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, Value: 50},
		{Time: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, Value: 70}}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Name: "cpu, disk",
		Functions: "sum_all",
		Step:      2 * 60 * 1000,
		From:      suite.basicQueryTime,
		To:        suite.basicQueryTime + 5*tsdbtest.MinuteInMillis}
	set, err := querierV2.Select(params)
	suite.Require().NoError(err, "Failed to execute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()

		data, err := tsdbtest.IteratorToSlice(iter)
		suite.NoError(err)

		suite.compareSingleMetric(data, expected)
	}

	suite.Require().Equal(2, seriesCount, "series count didn't match expected")
}

func (suite *testCrossSeriesAggregatesSuite) compareSingleMetric(data []tsdbtest.DataPoint, expected []tsdbtest.DataPoint) {
	for i, dataPoint := range data {
		suite.Require().True(dataPoint.Equals(expected[i]), "queried data does not match expected")
	}
}

func (suite *testCrossSeriesAggregatesSuite) compareSingleMetricWithAggregator(data []tsdbtest.DataPoint, expected map[string][]tsdbtest.DataPoint, agg string) {
	for i, dataPoint := range data {
		suite.Require().True(dataPoint.Equals(expected[agg][i]), "queried data does not match expected")
	}
}
