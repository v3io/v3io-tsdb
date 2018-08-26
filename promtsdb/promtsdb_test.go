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

package promtsdb

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/suite"
	"testing"
)

type testPromTsdbSuite struct {
	suite.Suite
}

func (suite *testPromTsdbSuite) TestMatch2filterEmpty() {

	name, filter, aggr := match2filter(nil)

	suite.Require().Equal("", name)
	suite.Require().Equal("", filter)
	suite.Require().Equal("", aggr)
}

func (suite *testPromTsdbSuite) TestMatch2filterEqual() {

	matchers := []*labels.Matcher{
		{Type: labels.MatchEqual, Name: "field", Value: "literal"},
	}
	name, filter, aggr := match2filter(matchers)

	suite.Require().Equal("", name)
	suite.Require().Equal("field=='literal'", filter)
	suite.Require().Equal("", aggr)
}

func (suite *testPromTsdbSuite) TestMatch2filterMultiple() {

	matchers := []*labels.Matcher{
		{Type: labels.MatchEqual, Name: "field1", Value: "literal1"},
		{Type: labels.MatchNotEqual, Name: "field2", Value: "literal2"},
	}
	name, filter, aggr := match2filter(matchers)

	suite.Require().Equal("", name)
	suite.Require().Equal("field1=='literal1' and field2!='literal2'", filter)
	suite.Require().Equal("", aggr)
}

func (suite *testPromTsdbSuite) TestMatch2filterMultipleWithName() {

	matchers := []*labels.Matcher{
		{Type: labels.MatchEqual, Name: "__name__", Value: "literal1"},
		{Type: labels.MatchNotEqual, Name: "field2", Value: "literal2"},
	}
	name, filter, aggr := match2filter(matchers)

	suite.Require().Equal("literal1", name)
	suite.Require().Equal("field2!='literal2'", filter)
	suite.Require().Equal("", aggr)
}

func (suite *testPromTsdbSuite) TestMatch2filterRegex() {

	matchers := []*labels.Matcher{
		{Type: labels.MatchRegexp, Name: "field", Value: ".*"},
	}
	name, filter, aggr := match2filter(matchers)

	suite.Require().Equal("", name)
	suite.Require().Equal(`regexp_instr(field,'.*') == 0`, filter)
	suite.Require().Equal("", aggr)
}

func (suite *testPromTsdbSuite) TestMatch2filterRegexMultiple() {

	matchers := []*labels.Matcher{
		{Type: labels.MatchRegexp, Name: "field1", Value: ".*"},
		{Type: labels.MatchNotRegexp, Name: "field2", Value: "..."},
	}
	name, filter, aggr := match2filter(matchers)

	suite.Require().Equal("", name)
	suite.Require().Equal(`regexp_instr(field1,'.*') == 0 and regexp_instr(field2,'...') != 0`, filter)
	suite.Require().Equal("", aggr)
}

func TestPromTsdbSuite(t *testing.T) {
	suite.Run(t, new(testPromTsdbSuite))
}
