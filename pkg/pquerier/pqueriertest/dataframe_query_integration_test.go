// +build integration

package pqueriertest

import (
	"errors"
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

			var expectedFloat float64
			switch val := expected[currentColAggregate].Value.(type) {
			case int:
				expectedFloat = float64(val)
			case float64:
				expectedFloat = val
			default:
				suite.Failf("invalid data type", "expected int or float, actual type is %t", val)
			}
			suite.Require().Equal(expectedFloat, f)
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
				switch val := currentExpected.(type) {
				case float64:
					fv, err := col.FloatAt(i)
					assert.NoError(suite.T(), err)
					if !(math.IsNaN(val) && math.IsNaN(fv)) {
						assert.Equal(suite.T(), currentExpected, fv)
					}
				case int:
					iv, err := col.FloatAt(i)
					assert.NoError(suite.T(), err)
					assert.Equal(suite.T(), float64(val), iv)
				case string:
					sv, err := col.StringAt(i)
					assert.NoError(suite.T(), err)
					assert.Equal(suite.T(), val, sv)
				default:
					assert.Error(suite.T(), errors.New("unsupported data type"))
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

				switch val := currentExpected.(type) {
				case float64:
					f, err := col.FloatAt(i)
					assert.NoError(suite.T(), err)
					if !(math.IsNaN(val) && math.IsNaN(f)) {
						assert.Equal(suite.T(), currentExpected, f)
					}
				case int:
					iv, err := col.FloatAt(i)
					assert.NoError(suite.T(), err)
					assert.Equal(suite.T(), float64(val), iv)
				case string:
					s, err := col.StringAt(i)
					assert.NoError(suite.T(), err)
					assert.Equal(suite.T(), val, s)
				default:
					assert.Error(suite.T(), errors.New("unsupported data type"))
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

				switch val := currentExpected.(type) {
				case float64:
					f, err := col.FloatAt(i)
					assert.NoError(suite.T(), err)
					if !(math.IsNaN(val) && math.IsNaN(f)) {
						assert.Equal(suite.T(), currentExpected, f)
					}
				case int:
					iv, err := col.FloatAt(i)
					assert.NoError(suite.T(), err)
					assert.Equal(suite.T(), float64(val), iv)
				case string:
					s, err := col.StringAt(i)
					assert.NoError(suite.T(), err)
					assert.Equal(suite.T(), val, s)
				default:
					assert.Error(suite.T(), errors.New("unsupported data type"))
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
				switch val := currentExpected.(type) {
				case float64:
					fv, err := col.FloatAt(i)
					assert.NoError(suite.T(), err)
					if !(math.IsNaN(val) && math.IsNaN(fv)) {
						assert.Equal(suite.T(), currentExpected, fv)
					}
				case int:
					iv, err := col.FloatAt(i)
					assert.NoError(suite.T(), err)
					assert.Equal(suite.T(), float64(val), iv)
				case string:
					sv, err := col.StringAt(i)
					assert.NoError(suite.T(), err)
					assert.Equal(suite.T(), val, sv)
				default:
					assert.Error(suite.T(), errors.New("unsupported data type"))
				}
			}
		}
	}

	suite.Require().Equal(1, seriesCount, "series count didn't match expected")
}

func (suite *testSelectDataframeSuite) TestQueryNonExistingMetric() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels := utils.LabelsFromStringList("os", "linux")
	cpuData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels,
				Data:   cpuData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu, tal",
		From: suite.basicQueryTime, To: suite.basicQueryTime + 4*tsdbtest.MinuteInMillis}
	iter, err := querierV2.SelectDataFrame(params)
	suite.Require().NoError(err)

	expectedData := map[string][]tsdbtest.DataPoint{
		"cpu": {{suite.basicQueryTime, 10},
			{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
			{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
			{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, 40}},
		"tal": {{suite.basicQueryTime, math.NaN()},
			{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), math.NaN()},
			{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, math.NaN()},
			{suite.basicQueryTime + 3*tsdbtest.MinuteInMillis, math.NaN()}}}

	var seriesCount int
	for iter.NextFrame() {
		seriesCount++
		frame, err := iter.GetFrame()
		suite.NoError(err)

		indexCol := frame.Indices()[0]
		assert.Equal(suite.T(), 4, indexCol.Len())
		for i := 0; i < indexCol.Len(); i++ {
			t, err := indexCol.TimeAt(i)
			assert.NoError(suite.T(), err)
			suite.Require().Equal(expectedData["cpu"][i].Time, t.UnixNano()/int64(time.Millisecond))

			for _, colName := range frame.Names() {
				col, err := frame.Column(colName)
				suite.NoError(err)
				currentExpectedData := expectedData[col.Name()]
				suite.Require().Equal(len(currentExpectedData), col.Len())
				currentExpected := currentExpectedData[i].Value

				switch val := currentExpected.(type) {
				case float64:
					f, err := col.FloatAt(i)
					assert.NoError(suite.T(), err)
					if !(math.IsNaN(val) && math.IsNaN(f)) {
						assert.Equal(suite.T(), currentExpected, f)
					}
				case int:
					iv, err := col.FloatAt(i)
					assert.NoError(suite.T(), err)
					assert.Equal(suite.T(), float64(val), iv)
				case string:
					s, err := col.StringAt(i)
					assert.NoError(suite.T(), err)
					assert.Equal(suite.T(), val, s)
				default:
					assert.Error(suite.T(), errors.New("unsupported data type"))
				}
			}
		}
	}
}

func (suite *testSelectDataframeSuite) TestSparseStringAndNumericColumnsDataframe() {
	requireCtx := suite.Require()
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	requireCtx.NoError(err, "failed to create v3io adapter")

	metricCpu := "cpu"
	metricLog := "log"
	labels := utils.LabelsFromStringList("os", "linux")
	labelsWithNameLog := append(labels, utils.LabelsFromStringList("__name__", metricLog)...)

	expectedTimeColumn := []int64{
		suite.basicQueryTime,
		suite.basicQueryTime + tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 2*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 3*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 4*tsdbtest.MinuteInMillis}

	timeColumnLog := []int64{
		suite.basicQueryTime,
		suite.basicQueryTime + 2*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 3*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 4*tsdbtest.MinuteInMillis}

	dataLog := []interface{}{"a", "c", "d", "e"}
	expectedColumns := map[string][]interface{}{
		metricCpu: {10.0, 20.0, 30.0, math.NaN(), 50.0},
		metricLog: {"a", "", "c", "d", "e"}}
	appender, err := adapter.Appender()
	requireCtx.NoError(err, "failed to create v3io appender")

	refLog, err := appender.Add(labelsWithNameLog, timeColumnLog[0], dataLog[0])
	suite.NoError(err, "failed to add data to the TSDB appender")
	for i := 1; i < len(timeColumnLog); i++ {
		appender.AddFast(labels, refLog, timeColumnLog[i], dataLog[i])
	}

	_, err = appender.WaitForCompletion(0)
	requireCtx.NoError(err, "failed to wait for TSDB append completion")

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   metricCpu,
				Labels: labels,
				Data: []tsdbtest.DataPoint{
					{suite.basicQueryTime, 10.0},
					{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20.0},
					{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30.0},
					{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 50.0}}},
			}})

	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	requireCtx.NoError(err, "failed to create querier")

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: metricCpu}, {Metric: metricLog}},
		From: suite.basicQueryTime, To: suite.basicQueryTime + 5*tsdbtest.MinuteInMillis}
	iter, err := querierV2.SelectDataFrame(params)
	requireCtx.NoError(err, "failed to execute query")

	var seriesCount int
	for iter.NextFrame() {
		seriesCount++
		frame, err := iter.GetFrame()
		requireCtx.NoError(err)
		indexCol := frame.Indices()[0]

		nullValuesMap := frame.NullValuesMap()
		requireCtx.NotNil(nullValuesMap, "null value map should not be empty")

		for i := 0; i < indexCol.Len(); i++ {
			t, _ := indexCol.TimeAt(i)
			timeMillis := t.UnixNano() / int64(time.Millisecond)
			requireCtx.Equal(expectedTimeColumn[i], timeMillis, "time column does not match at index %v", i)
			for _, columnName := range frame.Names() {
				var v interface{}
				column, err := frame.Column(columnName)
				requireCtx.NoError(err)
				if column.DType() == frames.FloatType {
					v, _ = column.FloatAt(i)
					if v == math.NaN() {
						requireCtx.True(nullValuesMap[i].NullColumns[columnName])
					}
					bothNaN := math.IsNaN(expectedColumns[column.Name()][i].(float64)) && math.IsNaN(v.(float64))
					if bothNaN {
						continue
					}
				} else if column.DType() == frames.StringType {
					v, _ = column.StringAt(i)
					if v == "" {
						requireCtx.True(nullValuesMap[i].NullColumns[columnName])
					}
				} else {
					suite.Fail(fmt.Sprintf("column type is not as expected: %v", column.DType()))
				}
				requireCtx.Equal(expectedColumns[column.Name()][i], v, "column %v does not match at index %v", column.Name(), i)
			}
		}
	}
}

func (suite *testSelectDataframeSuite) TestSparseNumericColumnsWithEmptyColumnsDataframe() {
	requireCtx := suite.Require()
	labelSetLinux := utils.LabelsFromStringList("os", "linux")
	labelSetWindows := utils.LabelsFromStringList("os", "windows")
	expectedTimeColumn := []int64{
		suite.basicQueryTime,
		suite.basicQueryTime + tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 2*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 3*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 4*tsdbtest.MinuteInMillis}
	expectedColumns := map[string][]interface{}{
		"cpu_0-linux":   {math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		"cpu_0-windows": {math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		"cpu_1-linux":   {10.0, 20.0, 30.0, math.NaN(), 50.0},
		"cpu_1-windows": {math.NaN(), 22.0, 33.0, math.NaN(), 55.0},
		"cpu_2-linux":   {math.NaN(), math.NaN(), math.NaN(), 40.4, 50.5},
		"cpu_2-windows": {10.0, 20.0, math.NaN(), 40.0, 50.0},
	}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{
				tsdbtest.Metric{
					Name:   "cpu_0",
					Labels: labelSetWindows,
					Data: []tsdbtest.DataPoint{ // out of test's time frame
						{expectedTimeColumn[0] - 68*tsdbtest.HoursInMillis, 10.0},
						{expectedTimeColumn[1] - 69*tsdbtest.HoursInMillis, 20.0},
						{expectedTimeColumn[2] - 70*tsdbtest.HoursInMillis, 30.0},
						{expectedTimeColumn[3] - 71*tsdbtest.HoursInMillis, 40.0},
						{expectedTimeColumn[4] - 72*tsdbtest.HoursInMillis, 50.0}}},
				tsdbtest.Metric{
					Name:   "cpu_1",
					Labels: labelSetLinux,
					Data: []tsdbtest.DataPoint{
						{expectedTimeColumn[0], 10.0},
						{expectedTimeColumn[1], 20.0},
						{expectedTimeColumn[2], 30.0},
						{expectedTimeColumn[4], 50.0}}},
				tsdbtest.Metric{
					Name:   "cpu_2",
					Labels: labelSetLinux,
					Data: []tsdbtest.DataPoint{
						// NA
						// NA
						{expectedTimeColumn[3], 40.4},
						{expectedTimeColumn[4], 50.5}}},
				tsdbtest.Metric{
					Name:   "cpu_2",
					Labels: labelSetWindows,
					Data: []tsdbtest.DataPoint{ // out of test's time frame
						{expectedTimeColumn[0], 10.0},
						{expectedTimeColumn[1], 20.0},
						// NA
						{expectedTimeColumn[3], 40.0},
						{expectedTimeColumn[4], 50.0}}},
				tsdbtest.Metric{
					Name:   "cpu_1",
					Labels: labelSetWindows,
					Data: []tsdbtest.DataPoint{ // out of test's time frame
						// NA
						{expectedTimeColumn[1], 22.0},
						{expectedTimeColumn[2], 33.0},
						// NA
						{expectedTimeColumn[4], 55.0}}},
			}})

	adapter := tsdbtest.InsertData(suite.T(), testParams)
	querierV2, err := adapter.QuerierV2()
	requireCtx.NoError(err, "failed to create querier")

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu_0"}, {Metric: "cpu_1"}, {Metric: "cpu_2"}},
		From: suite.basicQueryTime, To: suite.basicQueryTime + 10*tsdbtest.MinuteInMillis}
	iter, err := querierV2.SelectDataFrame(params)
	requireCtx.NoError(err, "failed to execute query")

	var seriesCount int
	for iter.NextFrame() {
		seriesCount++
		frame, err := iter.GetFrame()
		requireCtx.NoError(err)
		indexCol := frame.Indices()[0]
		osLabel := frame.Labels()["os"]

		nullValuesMap := frame.NullValuesMap()
		requireCtx.NotNil(nullValuesMap, "null value map should not be empty")

		for i := 0; i < indexCol.Len(); i++ {
			t, _ := indexCol.TimeAt(i)
			timeMillis := t.UnixNano() / int64(time.Millisecond)
			requireCtx.Equal(expectedTimeColumn[i], timeMillis, "time column does not match at index %v", i)
			for _, columnName := range frame.Names() {
				var v interface{}
				key := fmt.Sprintf("%v-%v", columnName, osLabel)
				column, err := frame.Column(columnName)
				requireCtx.NoError(err)
				if column.DType() == frames.FloatType {
					v, _ = column.FloatAt(i)
					if v == math.NaN() {
						requireCtx.True(nullValuesMap[i].NullColumns[columnName])
					}
					bothNaN := math.IsNaN(expectedColumns[key][i].(float64)) && math.IsNaN(v.(float64))
					if bothNaN {
						continue
					}
				} else if column.DType() == frames.StringType {
					v, _ = column.StringAt(i)
					if v == "" {
						requireCtx.True(nullValuesMap[i].NullColumns[columnName])
					}
				} else {
					suite.Fail(fmt.Sprintf("column type is not as expected: %v", column.DType()))
				}

				expectedValue := expectedColumns[key][i]
				if !math.IsNaN(expectedValue.(float64)) || !math.IsNaN(v.(float64)) {
					requireCtx.Equal(expectedValue, v, "column %v does not match at index %v", columnName, i)
				}
			}
		}
	}
}

func (suite *testSelectDataframeSuite) TestSparseNumericColumnsWithPartialLabelsDataframe() {
	requireCtx := suite.Require()
	labelSetLinux := utils.LabelsFromStringList("os", "linux")
	labelSetWindows := utils.LabelsFromStringList("os", "windows")
	expectedTimeColumn := []int64{
		suite.basicQueryTime,
		suite.basicQueryTime + tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 2*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 3*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 4*tsdbtest.MinuteInMillis}
	expectedColumns := map[string][]interface{}{
		"cpu_0-linux":   {math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		"cpu_0-windows": {math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		"cpu_1-linux":   {10.0, 20.0, 30.0, 40.0, 50.0},
		"cpu_1-windows": {math.NaN(), 22.0, 33.0, math.NaN(), 55.0},
		"cpu_2-linux":   {math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		"cpu_2-windows": {10.0, 20.0, math.NaN(), 40.0, 50.0},
	}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{
				tsdbtest.Metric{
					Name:   "cpu_0",
					Labels: labelSetWindows,
					Data: []tsdbtest.DataPoint{ // out of test's time frame
						{expectedTimeColumn[0] - 68*tsdbtest.HoursInMillis, 10.0},
						{expectedTimeColumn[1] - 69*tsdbtest.HoursInMillis, 20.0},
						{expectedTimeColumn[2] - 70*tsdbtest.HoursInMillis, 30.0},
						{expectedTimeColumn[3] - 71*tsdbtest.HoursInMillis, 40.0},
						{expectedTimeColumn[4] - 72*tsdbtest.HoursInMillis, 50.0}}},
				tsdbtest.Metric{
					Name:   "cpu_1",
					Labels: labelSetLinux,
					Data: []tsdbtest.DataPoint{
						{expectedTimeColumn[0], 10.0},
						{expectedTimeColumn[1], 20.0},
						{expectedTimeColumn[2], 30.0},
						{expectedTimeColumn[3], 40.0},
						{expectedTimeColumn[4], 50.0}}},
				tsdbtest.Metric{
					Name:   "cpu_2",
					Labels: labelSetWindows,
					Data: []tsdbtest.DataPoint{ // out of test's time frame
						{expectedTimeColumn[0], 10.0},
						{expectedTimeColumn[1], 20.0},
						// NA
						{expectedTimeColumn[3], 40.0},
						{expectedTimeColumn[4], 50.0}}},
				tsdbtest.Metric{
					Name:   "cpu_1",
					Labels: labelSetWindows,
					Data: []tsdbtest.DataPoint{ // out of test's time frame
						// NA
						{expectedTimeColumn[1], 22.0},
						{expectedTimeColumn[2], 33.0},
						// NA
						{expectedTimeColumn[4], 55.0}}},
			}})

	adapter := tsdbtest.InsertData(suite.T(), testParams)
	querierV2, err := adapter.QuerierV2()
	requireCtx.NoError(err, "failed to create querier")

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu_0"}, {Metric: "cpu_1"}, {Metric: "cpu_2"}},
		From: suite.basicQueryTime, To: suite.basicQueryTime + 10*tsdbtest.MinuteInMillis}
	iter, err := querierV2.SelectDataFrame(params)
	requireCtx.NoError(err, "failed to execute query")

	var seriesCount int
	for iter.NextFrame() {
		seriesCount++
		frame, err := iter.GetFrame()
		requireCtx.NoError(err)
		indexCol := frame.Indices()[0]
		osLabel := frame.Labels()["os"]

		nullValuesMap := frame.NullValuesMap()
		requireCtx.NotNil(nullValuesMap, "null value map should not be empty")

		for i := 0; i < indexCol.Len(); i++ {
			t, _ := indexCol.TimeAt(i)
			timeMillis := t.UnixNano() / int64(time.Millisecond)
			requireCtx.Equal(expectedTimeColumn[i], timeMillis, "time column does not match at index %v", i)
			for _, columnName := range frame.Names() {
				key := fmt.Sprintf("%v-%v", columnName, osLabel)
				var v interface{}
				column, err := frame.Column(columnName)
				requireCtx.NoError(err)
				if column.DType() == frames.FloatType {
					v, _ = column.FloatAt(i)
					if v == math.NaN() {
						requireCtx.True(nullValuesMap[i].NullColumns[columnName])
					}
					bothNaN := math.IsNaN(expectedColumns[key][i].(float64)) && math.IsNaN(v.(float64))
					if bothNaN {
						continue
					}
				} else if column.DType() == frames.StringType {
					v, _ = column.StringAt(i)
					if v == "" {
						requireCtx.True(nullValuesMap[i].NullColumns[columnName])
					}
				} else {
					suite.Fail(fmt.Sprintf("column type is not as expected: %v", column.DType()))
				}

				expectedValue := expectedColumns[key][i]
				if !math.IsNaN(expectedValue.(float64)) || !math.IsNaN(v.(float64)) {
					requireCtx.Equal(expectedValue, v, "column %v does not match at index %v", columnName, i)
				}
			}
		}
	}
}

func (suite *testSelectDataframeSuite) TestSparseNumericColumnsWithNotExistingMetricDataframe() {
	requireCtx := suite.Require()
	labelSetLinux := utils.LabelsFromStringList("os", "linux")
	labelSetWindows := utils.LabelsFromStringList("os", "windows")
	expectedTimeColumn := []int64{
		suite.basicQueryTime,
		suite.basicQueryTime + tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 2*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 3*tsdbtest.MinuteInMillis,
		suite.basicQueryTime + 4*tsdbtest.MinuteInMillis}
	expectedColumns := map[string][]interface{}{
		"cpu_0-linux":   {math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		"cpu_1-linux":   {10.0, 20.0, 30.0, 40.0, 50.0},
		"cpu_2-linux":   {math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		"fake-linux":    {math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		"cpu_0-windows": {math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		"cpu_1-windows": {math.NaN(), 22.0, 33.0, math.NaN(), 55.0},
		"cpu_2-windows": {10.0, 20.0, math.NaN(), 40.0, 50.0},
		"fake-windows":  {math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()},
	}

	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{
				tsdbtest.Metric{
					Name:   "cpu_0",
					Labels: labelSetWindows,
					Data: []tsdbtest.DataPoint{ // out of test's time frame
						{expectedTimeColumn[0] - 68*tsdbtest.HoursInMillis, 10.0},
						{expectedTimeColumn[1] - 69*tsdbtest.HoursInMillis, 20.0},
						{expectedTimeColumn[2] - 70*tsdbtest.HoursInMillis, 30.0},
						{expectedTimeColumn[3] - 71*tsdbtest.HoursInMillis, 40.0},
						{expectedTimeColumn[4] - 72*tsdbtest.HoursInMillis, 50.0}}},
				tsdbtest.Metric{
					Name:   "cpu_1",
					Labels: labelSetLinux,
					Data: []tsdbtest.DataPoint{
						{expectedTimeColumn[0], 10.0},
						{expectedTimeColumn[1], 20.0},
						{expectedTimeColumn[2], 30.0},
						{expectedTimeColumn[3], 40.0},
						{expectedTimeColumn[4], 50.0}}},
				tsdbtest.Metric{
					Name:   "cpu_2",
					Labels: labelSetWindows,
					Data: []tsdbtest.DataPoint{ // out of test's time frame
						{expectedTimeColumn[0], 10.0},
						{expectedTimeColumn[1], 20.0},
						// NA
						{expectedTimeColumn[3], 40.0},
						{expectedTimeColumn[4], 50.0}}},
				tsdbtest.Metric{
					Name:   "cpu_1",
					Labels: labelSetWindows,
					Data: []tsdbtest.DataPoint{ // out of test's time frame
						// NA
						{expectedTimeColumn[1], 22.0},
						{expectedTimeColumn[2], 33.0},
						// NA
						{expectedTimeColumn[4], 55.0}}},
			}})

	adapter := tsdbtest.InsertData(suite.T(), testParams)
	querierV2, err := adapter.QuerierV2()
	requireCtx.NoError(err, "failed to create querier")

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu_0"}, {Metric: "cpu_1"}, {Metric: "cpu_2"}, {Metric: "fake"}},
		From: suite.basicQueryTime, To: suite.basicQueryTime + 10*tsdbtest.MinuteInMillis}
	iter, err := querierV2.SelectDataFrame(params)
	requireCtx.NoError(err, "failed to execute query")

	var seriesCount int
	for iter.NextFrame() {
		seriesCount++
		frame, err := iter.GetFrame()
		requireCtx.NoError(err)
		indexCol := frame.Indices()[0]
		osLabel := frame.Labels()["os"]
		nullValuesMap := frame.NullValuesMap()
		requireCtx.NotNil(nullValuesMap, "null value map should not be empty")

		for i := 0; i < indexCol.Len(); i++ {
			t, _ := indexCol.TimeAt(i)
			timeMillis := t.UnixNano() / int64(time.Millisecond)
			requireCtx.Equal(expectedTimeColumn[i], timeMillis, "time column does not match at index %d", i)
			for _, columnName := range frame.Names() {
				key := fmt.Sprintf("%v-%v", columnName, osLabel)
				var v interface{}
				column, err := frame.Column(columnName)
				requireCtx.NoError(err)
				if column.DType() == frames.FloatType {
					v, _ = column.FloatAt(i)
					if v == math.NaN() {
						requireCtx.True(nullValuesMap[i].NullColumns[columnName])
					}

					bothNaN := math.IsNaN(expectedColumns[key][i].(float64)) && math.IsNaN(v.(float64))
					if bothNaN {
						continue
					}
				} else if column.DType() == frames.StringType {
					v, _ = column.StringAt(i)
					if v == "" {
						requireCtx.True(nullValuesMap[i].NullColumns[columnName])
					}
				} else {
					suite.Fail(fmt.Sprintf("column type is not as expected: %v", column.DType()))
				}

				expectedValue := expectedColumns[key][i]
				if !math.IsNaN(expectedValue.(float64)) || !math.IsNaN(v.(float64)) {
					requireCtx.Equal(expectedValue, v, "column %v does not match at index %d", columnName, i)
				}
			}
		}
	}
}
