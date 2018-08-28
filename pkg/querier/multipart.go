package querier

import (
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"sort"
)

func NewSetSorter(set SeriesSet) (SeriesSet, error) {
	sorter := setSorter{}
	sorter.set = set
	sorter.index = -1

	for set.Next() {
		s := set.At()
		i := sort.Search(len(sorter.list), func(i int) bool { return sorter.list[i].GetKey() >= s.GetKey() })
		sorter.list = append(sorter.list, nil)
		copy(sorter.list[i+1:], sorter.list[i:])
		sorter.list[i] = s
	}
	if set.Err() != nil {
		sorter.err = set.Err()
		return nil, set.Err()
	}

	return &sorter, nil
}

type setSorter struct {
	set   SeriesSet
	list  []Series
	index int
	err   error
}

func (s *setSorter) Next() bool {
	if s.index >= len(s.list)-1 {
		return false
	}
	s.index++
	return true
}

func (s *setSorter) At() Series {
	return s.list[s.index]
}

func (s *setSorter) Err() error { return s.err }

type IterSortMerger struct {
	iters        []SeriesSet
	done         []bool
	currKey      uint64
	currInvalids []bool
	currSeries   []Series
	err          error
}

// Merge sort multiple SeriesSets
func newIterSortMerger(sets []SeriesSet) (SeriesSet, error) {
	newMerger := IterSortMerger{}
	newMerger.iters = sets
	newMerger.done = make([]bool, len(sets))
	newMerger.currInvalids = make([]bool, len(sets))
	return &newMerger, nil
}

func (im *IterSortMerger) Next() bool {

	completed := true
	keyIsSet := false
	for i, iter := range im.iters {
		if !im.currInvalids[i] {
			im.done[i] = !iter.Next()
			if iter.Err() != nil {
				im.err = iter.Err()
				return false
			}
		}
		completed = completed && im.done[i]
		if !im.done[i] {
			key := iter.At().GetKey()
			if !keyIsSet {
				im.currKey = key
				keyIsSet = true
			} else if key < im.currKey {
				im.currKey = key
			}
		}
	}

	if completed {
		return false
	}

	im.currSeries = im.currSeries[:0]
	for i, iter := range im.iters {
		im.currInvalids[i] = true
		if !im.done[i] {
			if iter.At().GetKey() == im.currKey {
				im.currInvalids[i] = false
				im.currSeries = append(im.currSeries, iter.At())
			}
		}
	}

	return true
}

// return the current key and list of iterators containing it
func (im *IterSortMerger) At() Series {
	newSeries := mergedSeries{series: im.currSeries}
	return &newSeries
}

func (im *IterSortMerger) Err() error {
	return im.err
}

type mergedSeries struct {
	series []Series
}

func (m *mergedSeries) Labels() utils.Labels {
	return m.series[0].Labels()
}

func (m *mergedSeries) Iterator() SeriesIterator {
	return newMergedSeriesIterator(m.series...)
}

func (m *mergedSeries) GetKey() uint64 {
	return m.series[0].GetKey()
}

type mergedSeriesIterator struct {
	series []Series
	i      int
	cur    SeriesIterator
}

func newMergedSeriesIterator(s ...Series) *mergedSeriesIterator {
	return &mergedSeriesIterator{
		series: s,
		i:      0,
		cur:    s[0].Iterator(),
	}
}

func (it *mergedSeriesIterator) Seek(t int64) bool {
	// We just scan the merges series sequentially as they are already
	// pre-selected by relevant time and should be accessed sequentially anyway.
	for i, s := range it.series[it.i:] {
		cur := s.Iterator()
		if !cur.Seek(t) {
			continue
		}
		it.cur = cur
		it.i += i
		return true
	}
	return false
}

func (it *mergedSeriesIterator) Next() bool {
	if it.cur.Next() {
		return true
	}
	if err := it.cur.Err(); err != nil {
		return false
	}
	if it.i == len(it.series)-1 {
		return false
	}

	it.i++
	it.cur = it.series[it.i].Iterator()

	return it.Next()
}

func (it *mergedSeriesIterator) At() (t int64, v float64) {
	return it.cur.At()
}

func (it *mergedSeriesIterator) Err() error {
	return it.cur.Err()
}
