// +build unit

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

package tsdbctl

import (
	"github.com/stretchr/testify/suite"
	"math"
	"testing"
)

type testAddSuite struct {
	suite.Suite
}

func (suite *testAddSuite) TestStrToTV() {

	ts, vs, err := strToTV("1533814796000,1533894796000", "10.1,202")

	suite.Require().Nil(err)
	suite.Require().Equal(ts, []int64{1533814796000, 1533894796000})
	suite.Require().Equal(vs, []float64{10.1, 202})
}

func (suite *testAddSuite) TestStrToTVSpecialValues() {

	ts, vs, err := strToTV("1533814796000,1533894796000,1533899796000", "NaN,Inf,-Inf")

	suite.Require().Nil(err)
	suite.Require().Equal(ts, []int64{1533814796000, 1533894796000, 1533899796000})
	suite.Require().True(math.IsNaN(vs[0])) // NaN != NaN, so we have to check this explicitly
	suite.Require().Equal(vs[1:], []float64{math.Inf(1), math.Inf(-1)})
}

func (suite *testAddSuite) TestStrToTVInvalidValue() {

	ts, vs, err := strToTV("1533814796000,1533894796000,1533899796000", "1.2,5,z")

	suite.Require().Nil(ts)
	suite.Require().Nil(vs)
	suite.Require().Error(err)
}

func (suite *testAddSuite) TestStrToTVInvalidTime() {

	ts, vs, err := strToTV("1533814796000,1533894796000,abc", "1.2,5,5.1")

	suite.Require().Nil(ts)
	suite.Require().Nil(vs)
	suite.Require().Error(err)
}

func (suite *testAddSuite) TestStrToLabelsWithEmptyName() {

	labels, err := strToLabels("", "a=A,b=B")

	suite.Require().Nil(labels)
	suite.Require().Error(err)
}

func (suite *testAddSuite) TestStrToLabelsWithIllegalName() {

	labels, err := strToLabels("illegal-name", "a=A,b=B")

	suite.Require().Nil(labels)
	suite.Require().Error(err)
}

func (suite *testAddSuite) TestStrToLabelsWithIllegalLabel() {

	labels, err := strToLabels("valid_name", "a=A,b-b=B")

	suite.Require().Nil(labels)
	suite.Require().Error(err)
}

func TestAddSuite(t *testing.T) {
	suite.Run(t, new(testAddSuite))
}
