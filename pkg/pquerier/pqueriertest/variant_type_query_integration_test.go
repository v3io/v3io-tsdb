// +build integration

package pqueriertest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type testVariantTypeSuite struct {
	basicQueryTestSuite
}

func TestVariantTypeSuite(t *testing.T) {
	suite.Run(t, new(testVariantTypeSuite))
}

func (suite *testVariantTypeSuite) TestVariantTypeQueryWithDataFrame() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	metricName := "log"
	labels := utils.LabelsFromStringList("os", "linux", "__name__", metricName)

	dataToIngest := []string{"a", "b", "c", "d", "e"}
	numberOfEvents := len(dataToIngest)
	var expectedTimeColumn []int64
	for i := 0; i < numberOfEvents; i++ {
		expectedTimeColumn = append(expectedTimeColumn, suite.basicQueryTime+int64(i)*tsdbtest.MinuteInMillis)
	}

	appender, err := adapter.Appender()
	if err != nil {
		suite.T().Fatalf("failed to create v3io appender. reason: %s", err)
	}

	ref, err := appender.Add(labels, expectedTimeColumn[0], dataToIngest[0])
	if err != nil {
		suite.T().Fatalf("Failed to add data to the TSDB appender. Reason: %s", err)
	}
	for i := 1; i < numberOfEvents; i++ {
		appender.AddFast(labels, ref, expectedTimeColumn[i], dataToIngest[i])
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		suite.T().Fatalf("Failed to wait for TSDB append completion. Reason: %s", err)
	}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: metricName}},
		From: suite.basicQueryTime - tsdbtest.DaysInMillis, To: suite.basicQueryTime + tsdbtest.DaysInMillis}
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
				v, _ := column.StringAt(i)

				expected := dataToIngest[i]

				assert.Equal(suite.T(), expected, v, "column %v does not match at index %v", column.Name(), i)
			}
		}
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testVariantTypeSuite) TestVariantTypeQueryWithSeries() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	metricName := "log"
	labels := utils.LabelsFromStringList("os", "linux", "__name__", metricName)

	dataToIngest := []string{"a", "b", "c", "d", "e"}
	numberOfEvents := len(dataToIngest)
	var expectedTimeColumn []int64
	for i := 0; i < numberOfEvents; i++ {
		expectedTimeColumn = append(expectedTimeColumn, suite.basicQueryTime+int64(i)*tsdbtest.MinuteInMillis)
	}

	appender, err := adapter.Appender()
	if err != nil {
		suite.T().Fatalf("failed to create v3io appender. reason: %s", err)
	}

	ref, err := appender.Add(labels, expectedTimeColumn[0], dataToIngest[0])
	if err != nil {
		suite.T().Fatalf("Failed to add data to the TSDB appender. Reason: %s", err)
	}
	for i := 1; i < numberOfEvents; i++ {
		appender.AddFast(labels, ref, expectedTimeColumn[i], dataToIngest[i])
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		suite.T().Fatalf("Failed to wait for TSDB append completion. Reason: %s", err)
	}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: metricName}},
		From: suite.basicQueryTime - tsdbtest.DaysInMillis, To: suite.basicQueryTime + tsdbtest.DaysInMillis}
	iter, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}
	var seriesCount int
	for iter.Next() {
		seriesCount++
		iter := iter.At().Iterator()
		var i int
		for iter.Next() {
			t, v := iter.AtString()
			assert.Equal(suite.T(), expectedTimeColumn[i], t, "time does not match at index %v", i)
			assert.Equal(suite.T(), dataToIngest[i], v, "value does not match at index %v", i)
			i++
		}
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}
