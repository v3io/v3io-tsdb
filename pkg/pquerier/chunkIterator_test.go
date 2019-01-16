// +build integration

package pquerier_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

const baseTestTime = int64(1547510400000) // 15/01/2019 00:00:00

type testRawChunkIterSuite struct {
	suite.Suite
	v3ioConfig     *config.V3ioConfig
	suiteTimestamp int64
}

func (suite *testRawChunkIterSuite) SetupSuite() {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	suite.Require().NoError(err)

	suite.v3ioConfig = v3ioConfig
	suite.suiteTimestamp = time.Now().Unix()
}

func (suite *testRawChunkIterSuite) SetupTest() {
	suite.v3ioConfig.TablePath = fmt.Sprintf("%s-%v", suite.T().Name(), suite.suiteTimestamp)
	tsdbtest.CreateTestTSDB(suite.T(), suite.v3ioConfig)
}

func (suite *testRawChunkIterSuite) TearDownTest() {
	suite.v3ioConfig.TablePath = fmt.Sprintf("%s-%v", suite.T().Name(), suite.suiteTimestamp)
	if !suite.T().Failed() {
		tsdbtest.DeleteTSDB(suite.T(), suite.v3ioConfig)
	}
}

func (suite *testRawChunkIterSuite) TestRawChunkIteratorWithZeroValue() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err)

	labels1 := utils.LabelsFromStringList("os", "linux")
	numberOfEvents := 10
	eventsInterval := 60 * 1000
	ingestData := []tsdbtest.DataPoint{{baseTestTime, 10},
		{baseTestTime + tsdbtest.MinuteInMillis, 0},
		{baseTestTime + 2*tsdbtest.MinuteInMillis, 30},
		{baseTestTime + 3*tsdbtest.MinuteInMillis, 40}}
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
	suite.Require().NoError(err)

	params, _, _ := pquerier.ParseQuery("select cpu")
	params.From = baseTestTime
	params.To = baseTestTime + int64(numberOfEvents*eventsInterval)

	set, err := querierV2.Select(params)
	suite.Require().NoError(err)

	var seriesCount int
	for set.Next() {
		seriesCount++
		iter := set.At().Iterator().(*pquerier.RawChunkIterator)

		var index int
		for iter.Next() {
			t, v := iter.At()
			prevT, prevV := iter.PeakBack()

			suite.Require().Equal(ingestData[index].Time, t, "current time does not match")
			suite.Require().Equal(ingestData[index].Value, v, "current value does not match")

			if index > 0 {
				suite.Require().Equal(ingestData[index-1].Time, prevT, "current time does not match")
				suite.Require().Equal(ingestData[index-1].Value, prevV, "current value does not match")
			}
			index++
		}
	}

	suite.Require().Equal(1, seriesCount, "series count didn't match expected")
}

func TestRawChunkIterSuite(t *testing.T) {
	suite.Run(t, new(testRawChunkIterSuite))
}
