// +build integration

package pqueriertest

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type testSelectDataframeSuite struct {
	basicQueryTestSuite
}

func TestSelectDataframeSuite(t *testing.T) {
	suite.Run(t, new(testSelectDataframeSuite))
}

func (suite *testSelectDataframeSuite) TestAggregatesWithZeroStepSelectDataframe() {
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

func (suite *testSelectDataframeSuite) TestEmptyRawDataSelectDataframe() {
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

func (suite *testSelectDataframeSuite) Test2Series1EmptySelectDataframe() {
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

func (suite *testSelectDataframeSuite) TestStringAndFloatMetricsDataframe() {
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

func (suite *testSelectDataframeSuite) TestQueryDataFrameMultipleMetricsWithMultipleLabelSets() {
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

func (suite *testSelectDataframeSuite) TestSelectDataframeAggregationsMetricsHaveBigGaps() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	ingestedData1 := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(suite.basicQueryTime - 4*tsdbtest.DaysInMillis), 20}}

	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime - 1*tsdbtest.DaysInMillis, 30}}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu1",
				Labels: labels1,
				Data:   ingestedData1},
				tsdbtest.Metric{
					Name:   "cpu2",
					Labels: labels1,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expectedTime := []int64{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, suite.basicQueryTime - 4*tsdbtest.DaysInMillis, suite.basicQueryTime - 1*tsdbtest.DaysInMillis}
	expected := map[string][]float64{"count(cpu1)": {1, 1, math.NaN()},
		"count(cpu2)": {math.NaN(), math.NaN(), 1}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{
		Functions: "count",
		Step:      int64(tsdbtest.MinuteInMillis),
		From:      suite.basicQueryTime - 7*tsdbtest.DaysInMillis,
		To:        suite.basicQueryTime}
	set, err := querierV2.SelectDataFrame(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var dataFrameCount int
	for set.NextFrame() {
		dataFrameCount++
		frame, err := set.GetFrame()
		suite.Require().NoError(err)
		suite.Require().Equal(len(expected), len(frame.Names()), "number of columns in frame does not match")
		suite.Require().Equal(len(expectedTime), frame.Indices()[0].Len(), "columns size is not as expected")

		indexCol := frame.Indices()[0]

		for i := 0; i < len(expected); i++ {
			t, err := indexCol.TimeAt(i)
			timeMillis := t.UnixNano() / int64(time.Millisecond)
			suite.Require().NoError(err)
			suite.Require().Equal(expectedTime[i], timeMillis)

			for _, currName := range frame.Names() {
				currCol, err := frame.Column(currName)
				suite.Require().NoError(err)
				currVal, err := currCol.FloatAt(i)

				suite.Require().NoError(err)
				if !(math.IsNaN(currVal) && math.IsNaN(expected[currName][i])) {
					suite.Require().Equal(expected[currName][i], currVal)
				}
			}
		}
	}

	suite.Require().Equal(1, dataFrameCount, "series count didn't match expected")
}

func (suite *testSelectDataframeSuite) TestSelectDataframeDaownsampleMetricsHaveBigGaps() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	ingestedData1 := []tsdbtest.DataPoint{{suite.basicQueryTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(suite.basicQueryTime - 4*tsdbtest.DaysInMillis), 20}}

	ingestedData2 := []tsdbtest.DataPoint{{suite.basicQueryTime - 1*tsdbtest.DaysInMillis, 30}}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu1",
				Labels: labels1,
				Data:   ingestedData1},
				tsdbtest.Metric{
					Name:   "cpu2",
					Labels: labels1,
					Data:   ingestedData2},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expectedTime := []int64{suite.basicQueryTime - 7*tsdbtest.DaysInMillis,
		suite.basicQueryTime - 4*tsdbtest.DaysInMillis - 2*tsdbtest.MinuteInMillis,
		suite.basicQueryTime - 4*tsdbtest.DaysInMillis - 1*tsdbtest.MinuteInMillis,
		suite.basicQueryTime - 4*tsdbtest.DaysInMillis,
		suite.basicQueryTime - 1*tsdbtest.DaysInMillis - 2*tsdbtest.MinuteInMillis,
		suite.basicQueryTime - 1*tsdbtest.DaysInMillis - 1*tsdbtest.MinuteInMillis,
		suite.basicQueryTime - 1*tsdbtest.DaysInMillis}
	expected := map[string][]float64{"cpu1": {10, 20, 20, 20, math.NaN(), math.NaN(), math.NaN()},
		"cpu2": {math.NaN(), math.NaN(), math.NaN(), math.NaN(), 30, 30, 30}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{
		Step: int64(tsdbtest.MinuteInMillis),
		From: suite.basicQueryTime - 7*tsdbtest.DaysInMillis,
		To:   suite.basicQueryTime}
	set, err := querierV2.SelectDataFrame(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var dataFrameCount int
	for set.NextFrame() {
		dataFrameCount++
		frame, err := set.GetFrame()
		suite.Require().NoError(err)
		suite.Require().Equal(len(expected), len(frame.Names()), "number of columns in frame does not match")
		suite.Require().Equal(len(expectedTime), frame.Indices()[0].Len(), "columns size is not as expected")

		indexCol := frame.Indices()[0]

		for i := 0; i < len(expected); i++ {
			t, err := indexCol.TimeAt(i)
			timeMillis := t.UnixNano() / int64(time.Millisecond)
			suite.Require().NoError(err)
			suite.Require().Equal(expectedTime[i], timeMillis)

			for _, currName := range frame.Names() {
				currCol, err := frame.Column(currName)
				suite.Require().NoError(err)
				currVal, err := currCol.FloatAt(i)

				suite.Require().NoError(err)
				if !(math.IsNaN(currVal) && math.IsNaN(expected[currName][i])) {
					suite.Require().Equal(expected[currName][i], currVal)
				}
			}
		}
	}

	suite.Require().Equal(1, dataFrameCount, "series count didn't match expected")
}

func (suite *testSelectDataframeSuite) TestQueryDataFrameMultipleMetrics() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.NoError(err, "failed to create v3io adapter")

	metricName1 := "cpu"
	metricName2 := "diskio"
	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 5
	eventsInterval := int64(tsdbtest.MinuteInMillis)
	ingestData1 := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 15},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 18}}
	ingestData2 := []tsdbtest.DataPoint{{suite.basicQueryTime + tsdbtest.MinuteInMillis, 20},
		{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 22},
		{suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, 26}}

	expectedData := map[string][]tsdbtest.DataPoint{
		metricName1: {{suite.basicQueryTime, 10},
			{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, math.NaN()},
			{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 15},
			{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 18},
			{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, math.NaN()},
			{suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, math.NaN()}},
		metricName2: {{suite.basicQueryTime, math.NaN()},
			{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 20},
			{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, math.NaN()},
			{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, math.NaN()},
			{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 22},
			{suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, 26}}}

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
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	suite.NoError(err, "failed to create querier v2")

	params := &pquerier.SelectParams{Filter: "1==1",
		From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents)*eventsInterval}
	set, err := querierV2.SelectDataFrame(params)
	suite.NoError(err, "failed to exeute query")

	var seriesCount int
	for set.NextFrame() {
		seriesCount++
		frame, err := set.GetFrame()
		suite.NoError(err)

		indexCol := frame.Indices()[0]
		assert.Equal(suite.T(), 6, indexCol.Len())
		for i := 0; i < indexCol.Len(); i++ {
			t, err := indexCol.TimeAt(i)
			assert.NoError(suite.T(), err)
			suite.Require().Equal(expectedData[metricName1][i].Time, t.UnixNano()/int64(time.Millisecond))

			for _, colName := range frame.Names() {
				col, err := frame.Column(colName)
				suite.NoError(err)
				currentExpectedData := expectedData[col.Name()]
				suite.Require().Equal(len(currentExpectedData), col.Len())
				currentExpected := currentExpectedData[i].Value
				f, err := col.FloatAt(i)
				assert.NoError(suite.T(), err)

				if !(math.IsNaN(currentExpected) && math.IsNaN(f)) {
					suite.Require().Equal(currentExpected, f)
				}
			}
		}
	}

	suite.Require().Equal(1, seriesCount, "series count didn't match expected")
}

func (suite *testSelectDataframeSuite) TestColumnOrder() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.NoError(err, "failed to create v3io adapter")

	metricName1 := "cpu"
	metricName2 := "diskio"
	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 5
	eventsInterval := int64(tsdbtest.MinuteInMillis)
	ingestData1 := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 15},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 18}}
	ingestData2 := []tsdbtest.DataPoint{{suite.basicQueryTime + tsdbtest.MinuteInMillis, 20},
		{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 22},
		{suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, 26}}

	expectedData := map[string][]tsdbtest.DataPoint{
		metricName1: {{suite.basicQueryTime, 10},
			{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, math.NaN()},
			{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 15},
			{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 18},
			{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, math.NaN()},
			{suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, math.NaN()}},
		metricName2: {{suite.basicQueryTime, math.NaN()},
			{suite.basicQueryTime + 1*tsdbtest.MinuteInMillis, 20},
			{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, math.NaN()},
			{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, math.NaN()},
			{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 22},
			{suite.basicQueryTime + 5*tsdbtest.MinuteInMillis, 26}}}

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
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	suite.NoError(err, "failed to create querier v2")

	columnOrder := "diskio,cpu"
	params := &pquerier.SelectParams{Name: columnOrder,
		From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents)*eventsInterval}
	set, err := querierV2.SelectDataFrame(params)
	suite.NoError(err, "failed to exeute query")

	var seriesCount int
	for set.NextFrame() {
		seriesCount++
		frame, err := set.GetFrame()
		suite.NoError(err)

		indexCol := frame.Indices()[0]
		assert.Equal(suite.T(), 6, indexCol.Len())
		suite.Require().Equal(columnOrder, strings.Join(frame.Names(), ","))
		for i := 0; i < indexCol.Len(); i++ {
			t, err := indexCol.TimeAt(i)
			assert.NoError(suite.T(), err)
			suite.Require().Equal(expectedData[metricName1][i].Time, t.UnixNano()/int64(time.Millisecond))

			for _, colName := range frame.Names() {
				col, err := frame.Column(colName)
				suite.NoError(err)
				currentExpectedData := expectedData[col.Name()]
				suite.Require().Equal(len(currentExpectedData), col.Len())
				currentExpected := currentExpectedData[i].Value
				f, err := col.FloatAt(i)
				assert.NoError(suite.T(), err)

				if !(math.IsNaN(currentExpected) && math.IsNaN(f)) {
					suite.Require().Equal(currentExpected, f)
				}
			}
		}
	}

	suite.Require().Equal(1, seriesCount, "series count didn't match expected")
}
