// +build integration

package pqueriertest

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type testRawQuerySuite struct {
	basicQueryTestSuite
}

func TestRawQuerySuite(t *testing.T) {
	suite.Run(t, new(testRawQuerySuite))
}

func (suite *testRawQuerySuite) TestRawDataSinglePartition() {
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

func (suite *testRawQuerySuite) TestRawDataMultiplePartitions() {
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

func (suite *testRawQuerySuite) TestFilterOnLabel() {
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

func (suite *testRawQuerySuite) TestQueryWithBadTimeParameters() {
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

func (suite *testRawQuerySuite) TestQueryMetricWithDashInTheName() { // IG-8585
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

func (suite *testRawQuerySuite) TestSelectRawDataByRequestedColumns() {
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

func (suite *testRawQuerySuite) TestRawDataMultipleMetrics() {
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

func (suite *testRawQuerySuite) TestDataFrameRawDataMultipleMetrics() {
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

func (suite *testRawQuerySuite) TestQueryMultipleMetricsWithMultipleLabelSets() {
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

func (suite *testRawQuerySuite) TestDifferentLabelSetsInDifferentPartitions() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels2 := utils.LabelsFromStringList("os", "mac")

	ingestData2 := []tsdbtest.DataPoint{{suite.basicQueryTime - 9*tsdbtest.DaysInMillis - 1*tsdbtest.HoursInMillis, 40},
		{suite.basicQueryTime, 40}}

	expected := []tsdbtest.DataPoint{{suite.basicQueryTime, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels2,
				Data:   ingestData2},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{From: suite.basicQueryTime - 9*tsdbtest.DaysInMillis, To: suite.basicQueryTime + tsdbtest.DaysInMillis}
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

		suite.Require().Equal(expected, data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testRawQuerySuite) TestDifferentMetricsInDifferentPartitions() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")

	ingestData2 := []tsdbtest.DataPoint{{suite.basicQueryTime - 9*tsdbtest.DaysInMillis - 1*tsdbtest.HoursInMillis, 10},
		{suite.basicQueryTime, 40}}

	expected := []tsdbtest.DataPoint{{suite.basicQueryTime, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{
				tsdbtest.Metric{
					Name:   "diskio",
					Labels: labels1,
					Data:   ingestData2},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{From: suite.basicQueryTime - 9*tsdbtest.DaysInMillis, To: suite.basicQueryTime + tsdbtest.DaysInMillis}
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

		suite.Require().Equal(expected, data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testRawQuerySuite) TestQueryNonExistingMetric() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels := utils.LabelsFromStringList("os", "linux")
	cpuData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	diskioData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels,
				Data:   cpuData},
				tsdbtest.Metric{
					Name:   "diskio",
					Labels: labels,
					Data:   diskioData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu, tal", From: suite.basicQueryTime, To: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis}
	_, err = querierV2.SelectDataFrame(params)
	suite.Error(err, "expected error but finished successfully")

}

func (suite *testRawQuerySuite) TestQueryMetricDoesNotHaveData() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels := utils.LabelsFromStringList("os", "linux")
	cpuData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	diskioData := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels,
				Data:   cpuData},
				tsdbtest.Metric{
					Name:   "diskio",
					Labels: labels,
					Data:   diskioData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu, diskio",
		From: suite.basicQueryTime + tsdbtest.MinuteInMillis,
		To:   suite.basicQueryTime + 4*tsdbtest.MinuteInMillis}
	iter, err := querierV2.SelectDataFrame(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	expectedTimeColumn := []int64{suite.basicQueryTime + tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 2*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 3*tsdbtest.MinuteInMillis}
	expectedColumns := map[string][]float64{"cpu": {20, 30, 40},
		"diskio": {math.NaN(), math.NaN(), math.NaN()}}

	var seriesCount int
	for iter.NextFrame() {
		seriesCount++
		frame, err := iter.GetFrame()
		suite.NoError(err, "failed to get frame")
		indexCol := frame.Indices()[0] // in tsdb we have only one index
		suite.Require().Equal(len(expectedColumns), len(frame.Names()),
			"columns size does not match expected, got: %v", frame.Names())

		for i := 0; i < indexCol.Len(); i++ {
			t, _ := indexCol.TimeAt(i)
			timeMillis := t.UnixNano() / int64(time.Millisecond)
			suite.Require().Equal(expectedTimeColumn[i], timeMillis, "time column does not match at index %v", i)
			for _, columnName := range frame.Names() {
				column, err := frame.Column(columnName)
				suite.NoError(err)
				v, _ := column.FloatAt(i)

				expected := expectedColumns[columnName][i]

				// assert can not compare NaN, so we need to check it manually
				if !(math.IsNaN(expected) && math.IsNaN(v)) {
					suite.Require().Equal(expectedColumns[column.Name()][i], v, "column %v does not match at index %v", column.Name(), i)
				}
			}
		}
	}
}
