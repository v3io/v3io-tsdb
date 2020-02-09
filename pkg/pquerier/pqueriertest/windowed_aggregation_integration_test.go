// +build integration

package pqueriertest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type testWindowAggregationSuite struct {
	basicQueryTestSuite
}

func TestWindowAggregationSuite(t *testing.T) {
	suite.Run(t, new(testWindowAggregationSuite))
}

func (suite *testWindowAggregationSuite) TestClientWindowedAggregationWindowBiggerThanStep() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10

	var ingestedData []tsdbtest.DataPoint

	for i := 0; i < numberOfEvents; i++ {
		ingestedData = append(ingestedData, tsdbtest.DataPoint{Time: suite.basicQueryTime + int64(i)*tsdbtest.MinuteInMillis, Value: 10 * float64(i)})
	}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {
		{Time: suite.basicQueryTime, Value: 0},
		{Time: suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, Value: 150},
		{Time: suite.basicQueryTime + 10*tsdbtest.MinuteInMillis, Value: 390},
	}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions:         "sum",
		Step:              5 * tsdbtest.MinuteInMillis,
		AggregationWindow: 6 * tsdbtest.MinuteInMillis,
		From:              suite.basicQueryTime,
		To:                suite.basicQueryTime + 10*tsdbtest.MinuteInMillis}
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

func (suite *testWindowAggregationSuite) TestClientWindowedAggregationWindowSmallerThanStep() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10

	var ingestedData []tsdbtest.DataPoint

	for i := 0; i < numberOfEvents; i++ {
		ingestedData = append(ingestedData, tsdbtest.DataPoint{Time: suite.basicQueryTime + int64(i)*tsdbtest.MinuteInMillis, Value: 10 * float64(i)})
	}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {
		{Time: suite.basicQueryTime, Value: 0},
		{Time: suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, Value: 120},
		{Time: suite.basicQueryTime + 10*tsdbtest.MinuteInMillis, Value: 170},
	}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions:         "sum",
		Step:              5 * tsdbtest.MinuteInMillis,
		AggregationWindow: 2 * tsdbtest.MinuteInMillis,
		From:              suite.basicQueryTime,
		To:                suite.basicQueryTime + 10*tsdbtest.MinuteInMillis}
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

func (suite *testWindowAggregationSuite) TestClientWindowedAggregationWindowEqualToStep() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10

	var ingestedData []tsdbtest.DataPoint

	for i := 0; i < numberOfEvents; i++ {
		ingestedData = append(ingestedData, tsdbtest.DataPoint{Time: suite.basicQueryTime + int64(i)*tsdbtest.MinuteInMillis, Value: 10 * float64(i)})
	}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {
		{Time: suite.basicQueryTime, Value: 0},
		{Time: suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, Value: 150},
		{Time: suite.basicQueryTime + 10*tsdbtest.MinuteInMillis, Value: 300},
	}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions:         "sum",
		Step:              5 * tsdbtest.MinuteInMillis,
		AggregationWindow: 5 * tsdbtest.MinuteInMillis,
		From:              suite.basicQueryTime,
		To:                suite.basicQueryTime + 10*tsdbtest.MinuteInMillis}
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

func (suite *testWindowAggregationSuite) TestClientWindowedAggregationWindowExceedsPartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")

	ingestedData := []tsdbtest.DataPoint{{Time: suite.toMillis("2018-07-19T23:50:00Z"), Value: 1},
		{Time: suite.toMillis("2018-07-19T23:55:00Z"), Value: 2},
		{Time: suite.toMillis("2018-07-19T23:57:00Z"), Value: 3},
		{Time: suite.toMillis("2018-07-20T00:10:00Z"), Value: 4},
		{Time: suite.toMillis("2018-07-20T00:20:00Z"), Value: 5},
		{Time: suite.toMillis("2018-07-20T00:30:00Z"), Value: 6},
	}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {
		{Time: suite.toMillis("2018-07-20T00:10:00Z"), Value: 10},
		{Time: suite.toMillis("2018-07-20T00:20:00Z"), Value: 15},
		{Time: suite.toMillis("2018-07-20T00:30:00Z"), Value: 15},
	}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions:         "sum",
		Step:              10 * tsdbtest.MinuteInMillis,
		AggregationWindow: 30 * tsdbtest.MinuteInMillis,
		From:              suite.toMillis("2018-07-20T00:10:00Z"),
		To:                suite.toMillis("2018-07-20T00:30:00Z")}
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

func (suite *testWindowAggregationSuite) TestServerWindowedAggregationWindowBiggerThanStep() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10

	var ingestedData []tsdbtest.DataPoint

	for i := 0; i < numberOfEvents; i++ {
		ingestedData = append(ingestedData, tsdbtest.DataPoint{Time: suite.basicQueryTime + int64(i)*tsdbtest.HoursInMillis, Value: 10 * float64(i)})
	}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {
		{Time: suite.basicQueryTime, Value: 0},
		{Time: suite.basicQueryTime + 5*tsdbtest.HoursInMillis, Value: 150},
		{Time: suite.basicQueryTime + 10*tsdbtest.HoursInMillis, Value: 350},
	}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions:         "sum",
		Step:              5 * tsdbtest.HoursInMillis,
		AggregationWindow: 6 * tsdbtest.HoursInMillis,
		From:              suite.basicQueryTime,
		To:                suite.basicQueryTime + 10*tsdbtest.HoursInMillis}
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

func (suite *testWindowAggregationSuite) TestServerWindowedAggregationWindowEqualToStep() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10

	var ingestedData []tsdbtest.DataPoint

	for i := 0; i < numberOfEvents; i++ {
		ingestedData = append(ingestedData, tsdbtest.DataPoint{Time: suite.basicQueryTime + int64(i)*tsdbtest.HoursInMillis, Value: 10 * float64(i)})
	}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {
		{Time: suite.basicQueryTime, Value: 0},
		{Time: suite.basicQueryTime + 5*tsdbtest.HoursInMillis, Value: 150},
		{Time: suite.basicQueryTime + 10*tsdbtest.HoursInMillis, Value: 300},
	}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions:         "sum",
		Step:              5 * tsdbtest.HoursInMillis,
		AggregationWindow: 5 * tsdbtest.HoursInMillis,
		From:              suite.basicQueryTime,
		To:                suite.basicQueryTime + 10*tsdbtest.HoursInMillis}
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

func (suite *testWindowAggregationSuite) TestServerWindowedAggregationWindowEqualToRollupInterval() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10

	var ingestedData []tsdbtest.DataPoint

	for i := 0; i < numberOfEvents; i++ {
		ingestedData = append(ingestedData, tsdbtest.DataPoint{Time: suite.basicQueryTime + int64(i)*tsdbtest.HoursInMillis, Value: 10 * float64(i)})
	}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expected := map[string][]tsdbtest.DataPoint{"sum": {
		{Time: suite.basicQueryTime, Value: 0},
		{Time: suite.basicQueryTime + 1*tsdbtest.HoursInMillis, Value: 10},
		{Time: suite.basicQueryTime + 2*tsdbtest.HoursInMillis, Value: 20},
		{Time: suite.basicQueryTime + 3*tsdbtest.HoursInMillis, Value: 30},
		{Time: suite.basicQueryTime + 4*tsdbtest.HoursInMillis, Value: 40},
		{Time: suite.basicQueryTime + 5*tsdbtest.HoursInMillis, Value: 50},
		{Time: suite.basicQueryTime + 6*tsdbtest.HoursInMillis, Value: 60},
		{Time: suite.basicQueryTime + 7*tsdbtest.HoursInMillis, Value: 70},
		{Time: suite.basicQueryTime + 8*tsdbtest.HoursInMillis, Value: 80},
		{Time: suite.basicQueryTime + 9*tsdbtest.HoursInMillis, Value: 90},
	}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Functions:         "sum",
		Step:              1 * tsdbtest.HoursInMillis,
		AggregationWindow: 1 * tsdbtest.HoursInMillis,
		From:              suite.basicQueryTime,
		To:                suite.basicQueryTime + 10*tsdbtest.HoursInMillis}
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

func (suite *testWindowAggregationSuite) compareSingleMetricWithAggregator(data []tsdbtest.DataPoint, expected map[string][]tsdbtest.DataPoint, agg string) {
	for i, dataPoint := range data {
		suite.Require().True(dataPoint.Equals(expected[agg][i]), "queried data does not match expected")
	}
}