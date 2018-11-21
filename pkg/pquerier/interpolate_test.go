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

package pquerier

import (
	"math"
	"testing"

	"github.com/stretchr/testify/suite"
)

type testInterpolationSuite struct {
	suite.Suite
}

func (suite *testInterpolationSuite) TestNone() {
	fntype, err := StrToInterpolateType("")
	suite.Require().Nil(err)
	fn := GetInterpolateFunc(fntype)
	t, v := fn(10, 110, 60, 100, 200)
	suite.Require().Equal(t, int64(110))
	suite.Require().Equal(v, 200.0)
}

func (suite *testInterpolationSuite) TestNaN() {
	fntype, err := StrToInterpolateType("nan")
	suite.Require().Nil(err)
	fn := GetInterpolateFunc(fntype)
	t, v := fn(10, 110, 60, 100, 200)
	suite.Require().Equal(t, int64(60))
	suite.Require().Equal(math.IsNaN(v), true)
}

func (suite *testInterpolationSuite) TestPrev() {
	fntype, err := StrToInterpolateType("prev")
	suite.Require().Nil(err)
	fn := GetInterpolateFunc(fntype)
	t, v := fn(10, 110, 60, 100, 200)
	suite.Require().Equal(t, int64(60))
	suite.Require().Equal(v, 100.0)
}

func (suite *testInterpolationSuite) TestNext() {
	fntype, err := StrToInterpolateType("next")
	suite.Require().Nil(err)
	fn := GetInterpolateFunc(fntype)
	t, v := fn(10, 110, 60, 100, 200)
	suite.Require().Equal(t, int64(60))
	suite.Require().Equal(v, 200.0)
}

func (suite *testInterpolationSuite) TestLin() {
	fntype, err := StrToInterpolateType("lin")
	suite.Require().Nil(err)
	fn := GetInterpolateFunc(fntype)
	t, v := fn(10, 110, 60, 100, 200)
	suite.Require().Equal(t, int64(60))
	suite.Require().Equal(v, 150.0)
	t, v = fn(10, 110, 60, 100, math.NaN())
	suite.Require().Equal(t, int64(60))
	suite.Require().Equal(math.IsNaN(v), true)
}

func TestInterpolationSuite(t *testing.T) {
	suite.Run(t, new(testInterpolationSuite))
}
