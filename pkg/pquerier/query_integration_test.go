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

package pquerier_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"testing"
	"time"
)

type testQuerySuite struct {
	suite.Suite
	v3ioConfig *config.V3ioConfig
}

func (suite *testQuerySuite) SetupSuite() {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		suite.T().Fatalf("unable to load configuration. Error: %v", err)
	}

	suite.v3ioConfig = v3ioConfig
}

func (suite *testQuerySuite) SetupTest() {
	suite.v3ioConfig.TablePath = suite.T().Name()
	fmt.Println("creating:", suite.v3ioConfig.TablePath)
	tsdbtest.DeleteTSDB(suite.T(), suite.v3ioConfig)
	tsdbtest.CreateTestTSDB(suite.T(), suite.v3ioConfig)
}

func (suite *testQuerySuite) TeardownTest() {
	suite.v3ioConfig.TablePath = suite.T().Name()
	tsdbtest.DeleteTSDB(suite.T(), suite.v3ioConfig)
}

func (suite *testQuerySuite) TestRawDataSinglePartition() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	labels2 := utils.LabelsFromStrings("__name__", metricName, "os", "mac")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents*eventsInterval)
	expectedData := []tsdbtest.DataPoint{{baseTime, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*tsdbtest.MinuteInMillis, 30},
		{baseTime + 3*tsdbtest.MinuteInMillis, 40}}
	tsdbtest.InsertData(suite.T(), suite.v3ioConfig, metricName,
		expectedData,
		labels1)
	tsdbtest.InsertData(suite.T(), suite.v3ioConfig, metricName,
		expectedData,
		labels2)

	querierV2, err := adapter.QuerierV2(nil, baseTime, baseTime+int64(numberOfEvents*eventsInterval))
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu"}
	set, err := querierV2.SelectQry(params)
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

func (suite *testQuerySuite) TestRawDataMultiplePartitions() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	if err != nil {
		suite.T().Fatalf("failed to create v3io adapter. reason: %s", err)
	}
	metricName := "cpu"
	labels1 := utils.LabelsFromStrings("__name__", metricName, "os", "linux")
	labels2 := utils.LabelsFromStrings("__name__", metricName, "os", "mac")
	numberOfEvents := 5
	eventsInterval := int64(tsdbtest.MinuteInMillis)
	baseTime := time.Now().UnixNano()/1000000 - int64(numberOfEvents)*eventsInterval
	expectedData := []tsdbtest.DataPoint{{baseTime - 7*tsdbtest.DaysInMillis, 10},
		{int64(baseTime + tsdbtest.MinuteInMillis), 20},
		{baseTime + 2*eventsInterval, 30},
		{baseTime + 3*eventsInterval, 40}}
	tsdbtest.InsertData(suite.T(), suite.v3ioConfig, metricName,
		expectedData,
		labels1)
	tsdbtest.InsertData(suite.T(), suite.v3ioConfig, metricName,
		expectedData,
		labels2)

	querierV2, err := adapter.QuerierV2(nil,
		baseTime-8*tsdbtest.DaysInMillis, baseTime+int64(numberOfEvents)*eventsInterval)
	if err != nil {
		suite.T().Fatalf("Failed to create querier v2, err: %v", err)
	}

	params := &pquerier.SelectParams{Name: "cpu"}
	set, err := querierV2.SelectQry(params)
	if err != nil {
		suite.T().Fatalf("Failed to exeute query, err: %v", err)
	}

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator()
		data, err := tsdbtest.IteratorToSlice(iter)
		fmt.Println("actual data is: ", data)
		if err != nil {
			suite.T().Fatal(err)
		}

		assert.Equal(suite.T(), expectedData, data, "queried data does not match expected")
	}

	assert.Equal(suite.T(), 2, seriesCount, "series count didn't match expected")
}

func TestQueryV2Suite(t *testing.T) {
	suite.Run(t, new(testQuerySuite))
}
