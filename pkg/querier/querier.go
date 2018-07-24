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
	"github.com/nuclio/logger"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/partmgr"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"sort"
	"strings"
)

// Create a new Querier interface
func NewV3ioQuerier(container *v3io.Container, logger logger.Logger, mint, maxt int64,
	cfg *config.V3ioConfig, partMngr *partmgr.PartitionManager) *V3ioQuerier {
	newQuerier := V3ioQuerier{container: container, mint: mint, maxt: maxt,
		logger: logger.GetChild("Querier"), cfg: cfg}
	newQuerier.partitionMngr = partMngr
	return &newQuerier
}

type V3ioQuerier struct {
	logger        logger.Logger
	container     *v3io.Container
	cfg           *config.V3ioConfig
	mint, maxt    int64
	partitionMngr *partmgr.PartitionManager
	overlapWin    []int
}

// Standard Time Series Query, return a set of series which match the condition
func (q *V3ioQuerier) Select(name, functions string, step int64, filter string) (SeriesSet, error) {
	return q.selectQry(name, functions, step, nil, filter)
}

// Overlapping windows Time Series Query, return a set of series each with a list of aggregated results per window
// e.g. get the last 1hr, 6hr, 24hr stats per metric (specify a 1hr step of 3600*1000, 1,6,24 windows, and max time)
func (q *V3ioQuerier) SelectOverlap(name, functions string, step int64, win []int, filter string) (SeriesSet, error) {
	sort.Sort(sort.Reverse(sort.IntSlice(win)))
	return q.selectQry(name, functions, step, win, filter)
}

// base query function
func (q *V3ioQuerier) selectQry(name, functions string, step int64, win []int, filter string) (SeriesSet, error) {

	filter = strings.Replace(filter, "__name__", "_name", -1)
	q.logger.DebugWith("Select query", "func", functions, "step", step, "filter", filter)

	mint, maxt := q.mint, q.maxt
	if q.partitionMngr.IsCyclic() {
		partition := q.partitionMngr.GetHead()
		mint = partition.CyclicMinTime(mint, maxt)
		q.logger.DebugWith("Select - new cyclic series", "from", mint, "to", maxt, "name", name, "filter", filter)
		newSet := &V3ioSeriesSet{mint: mint, maxt: maxt, partition: partition, logger: q.logger}

		if functions != "" && step == 0 && partition.RollupTime() != 0 {
			step = partition.RollupTime()
		}

		newAggrSeries, err := aggregate.NewAggregateSeries(
			functions, "v", partition.AggrBuckets(), step, partition.RollupTime(), q.overlapWin)
		if err != nil {
			return nil, err
		}

		if newAggrSeries != nil && step != 0 {
			newSet.aggrSeries = newAggrSeries
			newSet.interval = step
			newSet.aggrIdx = newAggrSeries.NumFunctions() - 1
			newSet.overlapWin = q.overlapWin
		}

		err = newSet.getItems(partition.GetPath(), name, filter, q.container, q.cfg.QryWorkers)
		if err != nil {
			return nil, err
		}

		return newSet, nil

	}

	q.logger.Warn("Select with multi partitions, not supported ")
	// TODO: support multiple partitions
	//partitions := q.partitionMngr.PartsForRange(q.mint, q.maxt)

	return nullSeriesSet{}, nil
}

// return the current metric names
func (q *V3ioQuerier) LabelValues(name string) ([]string, error) {
	list := []string{}
	//for k, _ := range *q.Keymap {
	//	list = append(list, k)
	//}

	input := v3io.GetItemsInput{Path: q.cfg.Path + "/names/", AttributeNames: []string{"__name"}, Filter: ""}
	//iter, err := q.container.Sync.GetItemsCursor(&input)
	iter, err := utils.NewAsyncItemsCursor(q.container, &input, q.cfg.QryWorkers)
	q.logger.DebugWith("GetItems to read names", "input", input, "err", err)
	if err != nil {
		return list, err
	}

	for iter.Next() {
		name := iter.GetField("__name").(string)
		list = append(list, name)
	}

	if iter.Err() != nil {
		q.logger.InfoWith("Failed to read names, assume empty list", "err", iter.Err().Error())
	}
	return list, nil
}

func (q *V3ioQuerier) Close() error {
	return nil
}

func newSeriesSet(partition *partmgr.DBPartition, mint, maxt int64) *V3ioSeriesSet {

	return &V3ioSeriesSet{mint: mint, maxt: maxt, partition: partition}
}

// holds the query result set
type V3ioSeriesSet struct {
	err        error
	logger     logger.Logger
	partition  *partmgr.DBPartition
	iter       utils.ItemsCursor //*v3io.SyncItemsCursor
	mint, maxt int64
	attrs      []string
	chunkIds   []int

	interval   int64
	nullSeries bool
	overlapWin []int
	aggrSeries *aggregate.AggregateSeries
	aggrIdx    int
	currSeries Series
	aggrSet    *aggregate.AggregateSet
	baseTime   int64
}

// Get relevant items & attributes from the DB, and create an iterator
// TODO: get items per partition + merge, per partition calc attrs
func (s *V3ioSeriesSet) getItems(path, name, filter string, container *v3io.Container, workers int) error {

	attrs := []string{"_lset", "_meta", "_name", "_maxtime"}

	if s.aggrSeries != nil && s.aggrSeries.CanAggregate(s.partition.AggrType()) && s.maxt-s.mint >= s.interval {
		s.attrs = s.aggrSeries.GetAttrNames()
	} else {
		s.attrs, s.chunkIds = s.partition.Range2Attrs("v", s.mint, s.maxt)
	}
	attrs = append(attrs, s.attrs...)

	s.logger.DebugWith("Select - GetItems", "path", path, "attr", attrs, "filter", filter, "name", name)
	input := v3io.GetItemsInput{Path: path, AttributeNames: attrs, Filter: filter, ShardingKey: name}
	//iter, err := container.Sync.GetItemsCursor(&input)
	iter, err := utils.NewAsyncItemsCursor(container, &input, workers)
	//iter, err := utils.NewItemsCursor(container, &input)
	if err != nil {
		return err
	}

	s.iter = iter
	return nil

}

// advance to the next series
func (s *V3ioSeriesSet) Next() bool {

	// create raw chunks series (not aggregated)
	if s.aggrSeries == nil {
		if s.iter.Next() {
			s.currSeries = NewSeries(s)
			return true
		}
		return false
	}

	// create multiple aggregation series (one per aggregation function)
	if s.aggrIdx == s.aggrSeries.NumFunctions()-1 {
		if !s.iter.Next() {
			return false
		}

		s.nullSeries = false

		if s.aggrSeries.CanAggregate(s.partition.AggrType()) && s.maxt-s.mint > s.interval {

			// create series from aggregation arrays (in DB) if the partition stored the desired aggregates
			maxtUpdate := s.maxt
			maxTime := s.iter.GetField("_maxtime")
			if maxTime != nil && int64(maxTime.(int)) < s.maxt {
				maxtUpdate = int64(maxTime.(int))
			}
			mint := s.partition.CyclicMinTime(s.mint, maxtUpdate)

			start := s.partition.Time2Bucket(mint)
			end := s.partition.Time2Bucket(s.maxt + s.interval)

			// len of the returned array, cropped at the end in case of cyclic overlap
			length := int((maxtUpdate-mint)/s.interval) + 2

			if s.overlapWin != nil {
				s.baseTime = s.maxt //- int64(s.overlapWin[0]) * s.interval
			} else {
				s.baseTime = mint
			}

			if length > 0 {
				attrs := s.iter.GetFields()
				aggrSet, err := s.aggrSeries.NewSetFromAttrs(length, start, end, mint, s.maxt, &attrs)
				if err != nil {
					s.err = err
					return false
				}

				s.aggrSet = aggrSet
			} else {
				s.nullSeries = true
			}

		} else {

			// create series from raw chunks
			s.currSeries = NewSeries(s)
			s.aggrSet = s.aggrSeries.NewSetFromChunks(int((s.maxt-s.mint)/s.interval) + 1)
			if s.overlapWin != nil {
				s.chunks2WindowedAggregates()
			} else {
				s.chunks2IntervalAggregates()
			}

		}
	}

	s.aggrIdx = (s.aggrIdx + 1) % s.aggrSeries.NumFunctions()
	return true
}

// convert raw chunks to fixed interval aggragator
func (s *V3ioSeriesSet) chunks2IntervalAggregates() {

	iter := s.currSeries.Iterator()
	if iter.Next() {

		if iter.Err() != nil {
			s.err = iter.Err()
			return
		}

		t0, _ := iter.At()
		s.baseTime = (t0 / s.interval) * s.interval

		for {
			t, v := iter.At()
			s.aggrSet.AppendAllCells(int((t-s.baseTime)/s.interval), v)
			if !iter.Next() {
				// s.err = iter.Err()  // if the internal iterator has error we dont need to err the aggregator
				break
			}
		}
	}
}

// convert chunks to overlapping windows aggregator
func (s *V3ioSeriesSet) chunks2WindowedAggregates() {

	maxAligned := (s.maxt / s.interval) * s.interval
	//baseTime := maxAligned - int64(s.overlapWin[0])*s.interval

	iter := s.currSeries.Iterator()

	if iter.Seek(s.baseTime) {

		if iter.Err() != nil {
			s.err = iter.Err()
			return
		}

		s.baseTime = maxAligned

		for {
			t, v := iter.At()
			if t < maxAligned {
				for i, win := range s.overlapWin {
					if t > maxAligned-int64(win)*s.interval {
						s.aggrSet.AppendAllCells(i, v)
					}
				}
			}
			if !iter.Next() {
				s.err = iter.Err()
				break
			}
		}
	}
}

// return current error
func (s *V3ioSeriesSet) Err() error {
	if s.iter.Err() != nil {
		return s.iter.Err()
	}
	return s.err
}

// return a series iterator
func (s *V3ioSeriesSet) At() Series {
	if s.aggrSeries == nil {
		return s.currSeries
	}

	return NewAggrSeries(s, s.aggrSeries.GetFunctions()[s.aggrIdx])
}

// empty series set
type nullSeriesSet struct {
	err error
}

func (s nullSeriesSet) Next() bool { return false }
func (s nullSeriesSet) At() Series { return nil }
func (s nullSeriesSet) Err() error { return s.err }

// for future use, merge sort labels from multiple partitions
func mergeLables(a, b []string) []string {
	maxl := len(a)
	if len(b) > len(a) {
		maxl = len(b)
	}
	res := make([]string, 0, maxl*10/9)

	for len(a) > 0 && len(b) > 0 {
		d := strings.Compare(a[0], b[0])

		if d == 0 {
			res = append(res, a[0])
			a, b = a[1:], b[1:]
		} else if d < 0 {
			res = append(res, a[0])
			a = a[1:]
		} else if d > 0 {
			res = append(res, b[0])
			b = b[1:]
		}
	}

	// Append all remaining elements.
	res = append(res, a...)
	res = append(res, b...)
	return res
}
