// +build integration

package pqueriertest

import (
	"fmt"
	"strings"
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

type testSQLSyntaxQuerySuite struct {
	basicQueryTestSuite
}

func TestSQLSyntaxQuerySuite(t *testing.T) {
	suite.Run(t, new(testSQLSyntaxQuerySuite))
}

func (suite *testSQLSyntaxQuerySuite) TestGroupByOneLabelSinglePartition() {
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

func (suite *testSQLSyntaxQuerySuite) TestGroupByMultipleLabelsSinglePartition() {
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

func (suite *testSQLSyntaxQuerySuite) TestGroupByNotExistingLabel() {
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

func (suite *testSQLSyntaxQuerySuite) TestAggregateSeriesWithAlias() {
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

func (suite *testSQLSyntaxQuerySuite) TestAggregateSeriesWildcardOnPartOfTheColumns() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.NoError(err, "failed to create v3io adapter")

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
	expectedResult := map[string]float64{"max(cpu)": 40, "max(diskio)": 40, "min(cpu)": 10}

	querierV2, err := adapter.QuerierV2()
	suite.NoError(err, "failed to create querier v2")

	params, _, _ := pquerier.ParseQuery("select max(*), min(cpu)")

	params.From = suite.basicQueryTime
	params.To = suite.basicQueryTime + int64(numberOfEvents*eventsInterval)

	set, err := querierV2.Select(params)
	suite.NoError(err, "failed to exeute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		labels := set.At().Labels()
		expectedKey := fmt.Sprintf("%v(%v)", labels.Get(aggregate.AggregateLabel), labels.Get(config.PrometheusMetricNameAttribute))
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}
		suite.Require().Equal(1, len(data), "queried data does not match expected")
		suite.Require().Equal(expectedResult[expectedKey], data[0].Value, "queried data does not match expected")
	}

	suite.Require().Equal(len(expectedResult), seriesCount, "series count didn't match expected")
}

func (suite *testSQLSyntaxQuerySuite) TestAggregateSeriesWildcardOnPartOfTheColumnsWithVirtualColumn() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.NoError(err, "failed to create v3io adapter")

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
	expectedResult := map[string]float64{"avg(cpu)": 25, "avg(diskio)": 25, "min(cpu)": 10}

	querierV2, err := adapter.QuerierV2()
	suite.NoError(err, "failed to create querier v2")

	params, _, _ := pquerier.ParseQuery("select avg(*), min(cpu)")

	params.From = suite.basicQueryTime
	params.To = suite.basicQueryTime + int64(numberOfEvents*eventsInterval)

	set, err := querierV2.Select(params)
	suite.NoError(err, "failed to exeute query")

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		labels := set.At().Labels()
		expectedKey := fmt.Sprintf("%v(%v)", labels.Get(aggregate.AggregateLabel), labels.Get(config.PrometheusMetricNameAttribute))
		data, err := tsdbtest.IteratorToSlice(iter)
		if err != nil {
			suite.T().Fatal(err)
		}
		suite.Require().Equal(1, len(data), "queried data does not match expected")
		suite.Require().Equal(expectedResult[expectedKey], data[0].Value, "queried data does not match expected")
	}

	suite.Require().Equal(len(expectedResult), seriesCount, "series count didn't match expected")
}
