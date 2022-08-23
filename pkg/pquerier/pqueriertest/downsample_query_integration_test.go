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

type testDownsampleSuite struct {
	basicQueryTestSuite
}

func TestDownsampleSuite(t *testing.T) {
	suite.Run(t, new(testDownsampleSuite))
}

func (suite *testDownsampleSuite) TestDownSampleNotReturningAggrAttr() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 6*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 9*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu", Step: 2 * int64(tsdbtest.MinuteInMillis), From: suite.basicQueryTime, To: suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
	set, err := querierV2.Select(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		labels := set.At().Labels()
		suite.Require().Empty(labels.Get(aggregate.AggregateLabel))
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testDownsampleSuite) TestRawDataSinglePartitionWithDownSample() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000

	ingestData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{int64(suite.basicQueryTime + tsdbtest.MinuteInMillis), 20},
		{suite.basicQueryTime + 6*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 9*tsdbtest.MinuteInMillis, 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expectedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10},
		{suite.basicQueryTime + 2*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 4*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 6*tsdbtest.MinuteInMillis, 30},
		{suite.basicQueryTime + 8*tsdbtest.MinuteInMillis, 40}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu",
		Step: 2 * int64(tsdbtest.MinuteInMillis),
		From: suite.basicQueryTime,
		To:   suite.basicQueryTime + int64(numberOfEvents*eventsInterval)}
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

		suite.compareSingleMetric(data, expectedData)
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}

func (suite *testDownsampleSuite) TestRawDataDownSampleMultiPartitions() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}

	labels1 := utils.LabelsFromStringList("os", "linux")

	ingestData := []tsdbtest.DataPoint{{suite.toMillis("2018-11-18T23:40:00Z"), 10},
		{suite.toMillis("2018-11-18T23:59:00Z"), 20},
		{suite.toMillis("2018-11-19T00:20:00Z"), 30},
		{suite.toMillis("2018-11-19T02:40:00Z"), 40}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels1,
				Data:   ingestData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)

	expectedData := []tsdbtest.DataPoint{{suite.toMillis("2018-11-18T22:00:00Z"), 10},
		{suite.toMillis("2018-11-19T00:00:00Z"), 30},
		{suite.toMillis("2018-11-19T02:00:00Z"), 40}}

	querierV2, err := adapter.QuerierV2()
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "cpu"}},
		Step: 2 * int64(tsdbtest.HoursInMillis),
		From: suite.toMillis("2018-11-18T22:00:00Z"),
		To:   suite.toMillis("2018-11-19T4:00:00Z")}
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

		suite.compareSingleMetric(data, expectedData)
	}

	assert.Equal(suite.T(), 1, seriesCount, "series count didn't match expected")
}
