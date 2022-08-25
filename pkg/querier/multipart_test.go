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

package querier

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type testIterSortMergerSuite struct {
	suite.Suite
}

type mockSeriesSet struct {
	s    []utils.Series
	init bool
}

func (m *mockSeriesSet) Next() bool {
	if !m.init {
		m.init = true
	} else if len(m.s) > 1 {
		m.s = m.s[1:]
	} else {
		return false
	}
	return true
}

func (m *mockSeriesSet) At() utils.Series {
	return m.s[0]
}

func (m *mockSeriesSet) Err() error {
	return nil
}

type stubSeries uint64

func (stubSeries) Labels() utils.Labels {
	panic("stub")
}

func (stubSeries) Iterator() utils.SeriesIterator {
	panic("stub")
}

func (s stubSeries) GetKey() uint64 {
	return uint64(s)
}

func (suite *testIterSortMergerSuite) TestIterSortMerger() {

	s1 := []utils.Series{stubSeries(0), stubSeries(1)}
	s2 := []utils.Series{stubSeries(2), stubSeries(3)}
	iter, err := newIterSortMerger([]utils.SeriesSet{&mockSeriesSet{s: s1}, &mockSeriesSet{s: s2}})

	suite.Require().Nil(err)
	suite.Require().True(iter.Next())
	suite.Require().Equal(uint64(0), iter.At().GetKey())
	suite.Require().True(iter.Next())
	suite.Require().Equal(uint64(1), iter.At().GetKey())
	suite.Require().True(iter.Next())
	suite.Require().Equal(uint64(2), iter.At().GetKey())
	suite.Require().True(iter.Next())
	suite.Require().Equal(uint64(3), iter.At().GetKey())
}

func TestIterSortMergerSuiteSuite(t *testing.T) {
	suite.Run(t, new(testIterSortMergerSuite))
}
