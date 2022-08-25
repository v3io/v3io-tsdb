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

	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type getLabelSetsSuite struct {
	suite.Suite
	v3ioConfig     *config.V3ioConfig
	suiteTimestamp int64
	basicQueryTime int64
}

func TestGetLabelSetsSuite(t *testing.T) {
	suite.Run(t, new(getLabelSetsSuite))
}

func (suite *getLabelSetsSuite) SetupSuite() {
	v3ioConfig, err := tsdbtest.LoadV3ioConfig()
	if err != nil {
		suite.T().Fatalf("unable to load configuration. Error: %v", err)
	}

	suite.v3ioConfig = v3ioConfig
	suite.suiteTimestamp = time.Now().Unix()
	suite.basicQueryTime, err = tsdbtest.DateStringToMillis("2018-07-21T10:00:00Z")
	suite.NoError(err)
}

func (suite *getLabelSetsSuite) SetupTest() {
	suite.v3ioConfig.TablePath = fmt.Sprintf("%s-%v", suite.T().Name(), suite.suiteTimestamp)
	tsdbtest.CreateTestTSDB(suite.T(), suite.v3ioConfig)
}

func (suite *getLabelSetsSuite) TearDownTest() {
	suite.v3ioConfig.TablePath = fmt.Sprintf("%s-%v", suite.T().Name(), suite.suiteTimestamp)
	if !suite.T().Failed() {
		tsdbtest.DeleteTSDB(suite.T(), suite.v3ioConfig)
	}
}

func (suite *getLabelSetsSuite) TestGetLabels() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe"),
		utils.LabelsFromStringList("os", "linux", "region", "asia"),
		utils.LabelsFromStringList("os", "mac", "region", "europe")}
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels[0],
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[1],
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[2],
					Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedLabels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "linux", "region", "asia", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "mac", "region", "europe", config.PrometheusMetricNameAttribute, "cpu")}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	labelsList, err := querierV2.GetLabelSets("cpu", "")
	if err != nil {
		suite.T().Fatalf("failed to get label sets, err:%v\n", err)
	}

	suite.ElementsMatch(expectedLabels, labelsList, "actual label sets does not match expected")
}

func (suite *getLabelSetsSuite) TestGetLabelsAllMetrics() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe"),
		utils.LabelsFromStringList("os", "linux", "region", "asia"),
		utils.LabelsFromStringList("os", "mac", "region", "europe")}
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels[0],
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[1],
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "diskio",
					Labels: labels[2],
					Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedLabels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "linux", "region", "asia", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "mac", "region", "europe", config.PrometheusMetricNameAttribute, "diskio")}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	labelsList, err := querierV2.GetLabelSets("", "")
	if err != nil {
		suite.T().Fatalf("failed to get label sets, err:%v\n", err)
	}

	suite.ElementsMatch(expectedLabels, labelsList, "actual label sets does not match expected")
}

func (suite *getLabelSetsSuite) TestGetLabelsAllSpecificMetric() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe"),
		utils.LabelsFromStringList("os", "linux", "region", "asia"),
		utils.LabelsFromStringList("os", "mac", "region", "europe")}
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels[0],
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[1],
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "diskio",
					Labels: labels[2],
					Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedLabels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "linux", "region", "asia", config.PrometheusMetricNameAttribute, "cpu")}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	labelsList, err := querierV2.GetLabelSets("cpu", "")
	if err != nil {
		suite.T().Fatalf("failed to get label sets, err:%v\n", err)
	}

	suite.ElementsMatch(expectedLabels, labelsList, "actual label sets does not match expected")
}

func (suite *getLabelSetsSuite) TestGetLabelsWithFilter() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe"),
		utils.LabelsFromStringList("os", "linux", "region", "asia"),
		utils.LabelsFromStringList("os", "mac", "region", "europe")}
	ingestedData := []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels[0],
				Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[1],
					Data:   ingestedData},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[2],
					Data:   ingestedData},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedLabels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "linux", "region", "asia", config.PrometheusMetricNameAttribute, "cpu")}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	labelsList, err := querierV2.GetLabelSets("cpu", "os=='linux'")
	if err != nil {
		suite.T().Fatalf("failed to get label sets, err:%v\n", err)
	}

	suite.ElementsMatch(expectedLabels, labelsList, "actual label sets does not match expected")
}

func (suite *getLabelSetsSuite) TestGetLabelsAllMetricsFrom2Partitions() {
	adapter, err := tsdb.NewV3ioAdapter(suite.v3ioConfig, nil, nil)
	suite.Require().NoError(err, "failed to create v3io adapter")

	labels := []utils.Labels{utils.LabelsFromStringList("os", "linux", "region", "europe"),
		utils.LabelsFromStringList("os", "linux", "region", "asia"),
		utils.LabelsFromStringList("os", "mac", "region", "europe")}
	testParams := tsdbtest.NewTestParams(suite.T(),
		tsdbtest.TestOption{
			Key: tsdbtest.OptTimeSeries,
			Value: tsdbtest.TimeSeries{tsdbtest.Metric{
				Name:   "cpu",
				Labels: labels[0],
				Data:   []tsdbtest.DataPoint{{suite.basicQueryTime - 4*tsdbtest.DaysInMillis, 10}}},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[1],
					Data: []tsdbtest.DataPoint{{suite.basicQueryTime - 4*tsdbtest.DaysInMillis, 10},
						{suite.basicQueryTime - 2*tsdbtest.DaysInMillis, 10}}},
				tsdbtest.Metric{
					Name:   "cpu",
					Labels: labels[2],
					Data:   []tsdbtest.DataPoint{{suite.basicQueryTime, 10}}},
			}})
	tsdbtest.InsertData(suite.T(), testParams)
	expectedLabels := []utils.Labels{
		utils.LabelsFromStringList("os", "linux", "region", "asia", config.PrometheusMetricNameAttribute, "cpu"),
		utils.LabelsFromStringList("os", "mac", "region", "europe", config.PrometheusMetricNameAttribute, "cpu")}

	querierV2, err := adapter.QuerierV2()
	suite.Require().NoError(err, "failed to create querier v2")

	labelsList, err := querierV2.GetLabelSets("", "")
	if err != nil {
		suite.T().Fatalf("failed to get label sets, err:%v\n", err)
	}

	suite.Require().ElementsMatch(expectedLabels, labelsList, "actual label sets does not match expected")
}
