package querier

import (
	"sort"

	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

func NewSetSorter(set utils.SeriesSet) (utils.SeriesSet, error) {
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
	set   utils.SeriesSet
	list  []utils.Series
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

func (s *setSorter) At() utils.Series {
	return s.list[s.index]
}

func (s *setSorter) Err() error { return s.err }

type IterSortMerger struct {
	iters        []utils.SeriesSet
	done         []bool
	currKey      uint64
	currInvalids []bool
	currSeries   []utils.Series
	err          error
}

// Merge-sort multiple SeriesSets
func newIterSortMerger(sets []utils.SeriesSet) (utils.SeriesSet, error) {
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

	im.currSeries = make([]utils.Series, 0, len(im.iters))
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

// Return the current key and a list of iterators containing this key
func (im *IterSortMerger) At() utils.Series {
	newSeries := mergedSeries{series: im.currSeries}
	return &newSeries
}

func (im *IterSortMerger) Err() error {
	return im.err
}

type mergedSeries struct {
	series []utils.Series
}

func (m *mergedSeries) Labels() utils.Labels {
	return m.series[0].Labels()
}

func (m *mergedSeries) Iterator() utils.SeriesIterator {
	return newMergedSeriesIterator(m.series...)
}

func (m *mergedSeries) GetKey() uint64 {
	return m.series[0].GetKey()
}

type mergedSeriesIterator struct {
	series []utils.Series
	i      int
	cur    utils.SeriesIterator
}

func newMergedSeriesIterator(s ...utils.Series) *mergedSeriesIterator {
	return &mergedSeriesIterator{
		series: s,
		i:      0,
		cur:    s[0].Iterator(),
	}
}

func (it *mergedSeriesIterator) Seek(t int64) bool {
	// We just scan the merge series sequentially, as they are already
	// pre-selected by time and should be accessed sequentially anyway.
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

func (it *mergedSeriesIterator) AtString() (t int64, v string) { return it.cur.AtString() }

func (it *mergedSeriesIterator) Err() error {
	return it.cur.Err()
}

func (it *mergedSeriesIterator) Encoding() chunkenc.Encoding {
	return chunkenc.EncXOR
}
