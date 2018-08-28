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
	s    []Series
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

func (m *mockSeriesSet) At() Series {
	return m.s[0]
}

func (m *mockSeriesSet) Err() error {
	return nil
}

type stubSeries uint64

func (stubSeries) Labels() utils.Labels {
	panic("stub")
}

func (stubSeries) Iterator() SeriesIterator {
	panic("stub")
}

func (s stubSeries) GetKey() uint64 {
	return uint64(s)
}

func (suite *testIterSortMergerSuite) TestIterSortMerger() {

	s1 := []Series{stubSeries(0), stubSeries(1)}
	s2 := []Series{stubSeries(2), stubSeries(3)}
	iter, err := newIterSortMerger([]SeriesSet{&mockSeriesSet{s: s1}, &mockSeriesSet{s: s2}})

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

func TestAddSuite(t *testing.T) {
	suite.Run(t, new(testIterSortMergerSuite))
}
