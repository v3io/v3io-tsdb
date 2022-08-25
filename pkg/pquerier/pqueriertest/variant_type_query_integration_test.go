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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
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
		appender.AddFast(ref, expectedTimeColumn[i], dataToIngest[i])
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
		appender.AddFast(ref, expectedTimeColumn[i], dataToIngest[i])
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

func (suite *testVariantTypeSuite) TestCountAggregationForVariantTypeQueryWithSeries() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	metricName := "log"
	labels := utils.LabelsFromStringList("os", "linux", "__name__", metricName)

	dataToIngest := []string{"a", "b", "c", "d", "e", "f"}
	numberOfEvents := len(dataToIngest)
	var expectedTimeColumn []int64
	for i := 0; i < numberOfEvents; i++ {
		expectedTimeColumn = append(expectedTimeColumn, suite.basicQueryTime+int64(i)*tsdbtest.MinuteInMillis)
	}

	expected := map[string][]tsdbtest.DataPoint{"count": {{Time: suite.basicQueryTime - 5*tsdbtest.MinuteInMillis, Value: numberOfEvents}}}

	appender, err := adapter.Appender()
	if err != nil {
		suite.T().Fatalf("failed to create v3io appender. reason: %s", err)
	}

	ref, err := appender.Add(labels, expectedTimeColumn[0], dataToIngest[0])
	if err != nil {
		suite.T().Fatalf("Failed to add data to the TSDB appender. Reason: %s", err)
	}
	for i := 1; i < numberOfEvents; i++ {
		appender.AddFast(ref, expectedTimeColumn[i], dataToIngest[i])
	}

	if _, err := appender.WaitForCompletion(0); err != nil {
		suite.T().Fatalf("Failed to wait for TSDB append completion. Reason: %s", err)
	}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{
		From:      suite.basicQueryTime - tsdbtest.DaysInMillis,
		To:        suite.basicQueryTime + tsdbtest.DaysInMillis,
		Functions: "count",
		Step:      10 * tsdbtest.MinuteInMillis}

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
		labels := set.At().Labels()
		agg := labels.Get(aggregate.AggregateLabel)

		suite.compareSingleMetricWithAggregator(data, expected, agg)
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testVariantTypeSuite) TestVariantTypeQueryWithSeriesAlotOfData() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	metricName := "log"
	labels := utils.LabelsFromStringList("os", "linux", "__name__", metricName)

	numberOfEvents := 1000
	dataToIngest := make([]tsdbtest.DataPoint, numberOfEvents)
	for i := 0; i < numberOfEvents; i++ {
		dataToIngest[i] = tsdbtest.DataPoint{Time: suite.basicQueryTime + int64(i)*tsdbtest.MinuteInMillis,
			Value: fmt.Sprintf("%v", i)}
	}

	appender, err := adapter.Appender()
	if err != nil {
		suite.T().Fatalf("failed to create v3io appender. reason: %s", err)
	}

	ref, err := appender.Add(labels, dataToIngest[0].Time, dataToIngest[0].Value)
	if err != nil {
		suite.T().Fatalf("Failed to add data to the TSDB appender. Reason: %s", err)
	}
	for i := 1; i < numberOfEvents; i++ {
		appender.AddFast(ref, dataToIngest[i].Time, dataToIngest[i].Value)
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
		var slice []tsdbtest.DataPoint
		for iter.Next() {
			t, v := iter.AtString()
			slice = append(slice, tsdbtest.DataPoint{Time: t, Value: v})
		}

		suite.Require().Equal(dataToIngest, slice, "number of events mismatch")
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}
