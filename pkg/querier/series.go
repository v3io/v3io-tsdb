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
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
)

// Create a new series from chunks
func NewSeries(set *V3ioSeriesSet) Series {
	newSeries := V3ioSeries{set: set}
	newSeries.lset = initLabels(set)
	newSeries.initSeriesIter()
	return &newSeries
}

type V3ioSeries struct {
	set  *V3ioSeriesSet
	lset utils.Labels
	iter SeriesIterator
	hash uint64
}

func (s *V3ioSeries) Labels() utils.Labels { return s.lset }

// Get the unique series key for sorting
func (s *V3ioSeries) GetKey() uint64 {
	if s.hash == 0 {
		s.hash = s.lset.HashWithMetricName()
	}
	return s.hash
}

func (s *V3ioSeries) Iterator() SeriesIterator { return s.iter }

// Initialize the label set from _lset and _name attributes
func initLabels(set *V3ioSeriesSet) utils.Labels {
	name, nok := set.iter.GetField("_name").(string)
	if !nok {
		name = "UNKNOWN"
	}
	lsetAttr, lok := set.iter.GetField("_lset").(string)
	if !lok {
		lsetAttr = "UNKNOWN"
	}
	if !lok || !nok {
		set.logger.Error("Error in initLabels; bad field values.")
	}

	lset := utils.Labels{utils.Label{Name: "__name__", Value: name}}

	splitLset := strings.Split(lsetAttr, ",")
	for _, label := range splitLset {
		kv := strings.Split(label, "=")
		if len(kv) > 1 {
			lset = append(lset, utils.Label{Name: kv[0], Value: kv[1]})
		}
	}

	return lset
}

// Initialize the series from values, metadata, and attributes
func (s *V3ioSeries) initSeriesIter() {

	maxt := s.set.maxt
	maxTime := s.set.iter.GetField("_maxtime")
	if maxTime != nil && int64(maxTime.(int)) < maxt {
		maxt = int64(maxTime.(int))
	}

	newIterator := v3ioSeriesIterator{
		mint: s.set.mint, maxt: maxt}
	newIterator.chunks = []chunkenc.Chunk{}
	newIterator.chunksMax = []int64{}

	// Create and initialize a chunk encoder per chunk blob
	for i, attr := range s.set.attrs {
		values := s.set.iter.GetField(attr)

		if values != nil {
			bytes := values.([]byte)
			chunk, err := chunkenc.FromData(s.set.logger, chunkenc.EncXOR, bytes, 0)
			if err != nil {
				s.set.logger.ErrorWith("Error reading chunk buffer", "Lset", s.lset, "err", err)
			} else {
				newIterator.chunks = append(newIterator.chunks, chunk)
				newIterator.chunksMax = append(newIterator.chunksMax,
					s.set.chunk0Time+int64(i+1)*s.set.partition.TimePerChunk()-1)
			}
		}

	}

	if len(newIterator.chunks) == 0 {
		// If there's no data, create a null iterator
		s.iter = &nullSeriesIterator{}
	} else {
		newIterator.iter = newIterator.chunks[0].Iterator()
		s.iter = &newIterator
	}
}

// Chunk-list series iterator
type v3ioSeriesIterator struct {
	mint, maxt int64 // TBD per block
	err        error

	chunks []chunkenc.Chunk

	chunkIndex int
	chunksMax  []int64
	iter       chunkenc.Iterator
}

// Advance the iterator to the specified chunk and time
func (it *v3ioSeriesIterator) Seek(t int64) bool {

	// Seek time is after the item's end time (maxt)
	if t > it.maxt {
		return false
	}

	// Seek to the first valid value after t
	if t < it.mint {
		t = it.mint
	}

	// Check the first element
	t0, _ := it.iter.At()
	if t0 > it.maxt {
		return false
	}
	if t <= t0 {
		return true
	}

	for {
		if it.iter.Next() {
			t0, _ := it.iter.At()
			if t0 > it.maxt {
				return false
			}
			if t > it.chunksMax[it.chunkIndex] {
				// This chunk is too far behind; move to the next chunk or
				// Return false if it's the last chunk
				if it.chunkIndex == len(it.chunks)-1 {
					return false
				}
				it.chunkIndex++
				it.iter = it.chunks[it.chunkIndex].Iterator()
			} else if t <= t0 {
				// The cursor (t0) is either on t or just passed t
				return true
			}
		} else {
			// End of chunk; move to the next chunk or return if last
			if it.chunkIndex == len(it.chunks)-1 {
				return false
			}
			it.chunkIndex++
			it.iter = it.chunks[it.chunkIndex].Iterator()
		}
	}
}

// Move to the next iterator item
func (it *v3ioSeriesIterator) Next() bool {
	if it.iter.Next() {
		t, _ := it.iter.At()
		if t < it.mint {
			if !it.Seek(it.mint) {
				return false
			}
			t, _ = it.At()

			return t <= it.maxt
		}
		if t <= it.maxt {
			return true
		}
		return false
	}

	if err := it.iter.Err(); err != nil {
		return false
	}
	if it.chunkIndex == len(it.chunks)-1 {
		return false
	}

	it.chunkIndex++
	it.iter = it.chunks[it.chunkIndex].Iterator()
	return it.Next()
}

// Read the time and value at the current location
func (it *v3ioSeriesIterator) At() (t int64, v float64) { return it.iter.At() }

func (it *v3ioSeriesIterator) Err() error { return it.iter.Err() }

// Aggregates (count, avg, sum, ..) series and iterator

func NewAggrSeries(set *V3ioSeriesSet, aggr aggregate.AggrType) *V3ioSeries {
	newSeries := V3ioSeries{set: set}
	lset := initLabels(set)
	if !set.noAggrLbl {
		lset = append(lset, utils.Label{Name: aggregate.AggregateLabel, Value: aggr.String()})
	}
	newSeries.lset = lset

	if set.nullSeries {
		newSeries.iter = &nullSeriesIterator{}
	} else {

		// `set` - the iterator "iterates" over stateful data - it holds a
		// "current" set and aggrSet. This requires copying all the required
		// stateful data into the iterator (e.g., aggrSet) so that when it's
		// evaluated it will hold the proper pointer.
		newSeries.iter = &aggrSeriesIterator{
			set:      set,
			aggrSet:  set.aggrSet,
			aggrType: aggr,
			index:    -1,
		}
	}

	return &newSeries
}

type aggrSeriesIterator struct {
	set      *V3ioSeriesSet
	aggrSet  *aggregate.AggregateSet
	aggrType aggregate.AggrType
	index    int
	err      error
}

// Advance an iterator to the specified time (t)
func (s *aggrSeriesIterator) Seek(t int64) bool {
	if t <= s.set.baseTime {
		s.index = s.getNextValidCell(-1)
		return true
	}

	if t > s.set.baseTime+int64(s.aggrSet.GetMaxCell())*s.set.interval {
		return false
	}

	s.index = int((t - s.set.baseTime) / s.set.interval)
	return true
}

// Advance an iterator to the next time interval/bucket
func (s *aggrSeriesIterator) Next() bool {
	// Advance the index to the next non-empty cell
	s.index = s.getNextValidCell(s.index)
	return s.index <= s.aggrSet.GetMaxCell()
}

func (s *aggrSeriesIterator) getNextValidCell(from int) (nextIndex int) {
	for nextIndex = from + 1; nextIndex <= s.aggrSet.GetMaxCell() && !s.aggrSet.HasData(nextIndex); nextIndex++ {
	}
	return
}

// Return the time and value at the current bucket
func (s *aggrSeriesIterator) At() (t int64, v float64) {
	val, _ := s.aggrSet.GetCellValue(s.aggrType, s.index)
	return s.aggrSet.GetCellTime(s.set.baseTime, s.index), val
}

func (s *aggrSeriesIterator) Err() error { return s.err }

// Null-series iterator
type nullSeriesIterator struct {
	err error
}

func (s nullSeriesIterator) Seek(t int64) bool        { return false }
func (s nullSeriesIterator) Next() bool               { return false }
func (s nullSeriesIterator) At() (t int64, v float64) { return 0, 0 }
func (s nullSeriesIterator) Err() error               { return s.err }
