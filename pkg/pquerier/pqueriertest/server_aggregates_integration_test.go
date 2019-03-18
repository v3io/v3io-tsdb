// +build integration

package pquerier_test

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

type testServerAggregatesSuite struct {
	basicQueryTestSuite
}

func TestServerAggregatesSuite(t *testing.T) {
	suite.Run(t, new(testServerAggregatesSuite))
}

func (suite *testServerAggregatesSuite) TestRawAggregatesSinglePartition() {
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

func (suite *testServerAggregatesSuite) TestRawAggregatesWithQueryIntervalSameAsAggrGranularity() {
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

func (suite *testServerAggregatesSuite) TestRawAggregatesSinglePartitionNegativeValues() {
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

func (suite *testServerAggregatesSuite) TestRawAggregatesMultiPartition() {
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

func (suite *testServerAggregatesSuite) TestRawAggregatesMultiPartitionNonConcreteAggregates() {
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

func (suite *testServerAggregatesSuite) TestSelectServerAggregatesAndRawByRequestedColumns() {
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

func (suite *testServerAggregatesSuite) TestAggregatesWithDisabledClientAggregation() {
	suite.v3ioConfig.DisableClientAggr = true
	defer func() { suite.v3ioConfig.DisableClientAggr = false }()

	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime - tsdbtest.DaysInMillis, 10},
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

	expected := map[string][]tsdbtest.DataPoint{"avg": {{Time: suite.basicQueryTime - tsdbtest.DaysInMillis, Value: 10},
		{Time: suite.basicQueryTime, Value: 30}}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Functions: "avg", From: suite.basicQueryTime - tsdbtest.DaysInMillis, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
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
		currentExpected, ok := expected[agg]
		suite.Require().Equal(true, ok, "got unexpected aggregate result")
		assert.Equal(suite.T(), currentExpected, data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}
