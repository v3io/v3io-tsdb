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
	"math"
	"strings"
)

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
}

func (s *V3ioSeries) Labels() utils.Labels     { return s.lset }
func (s *V3ioSeries) Iterator() SeriesIterator { return s.iter }

// initialize the label set from _lset & name attributes
func initLabels(set *V3ioSeriesSet) utils.Labels {
	name := set.iter.GetField("_name").(string)
	lsetAttr := set.iter.GetField("_lset").(string)
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

// initialize the series from value metadata & attributes
func (s *V3ioSeries) initSeriesIter() {

	maxt := s.set.maxt
	maxTime := s.set.iter.GetField("_maxtime")
	if maxTime != nil && int64(maxTime.(int)) < maxt {
		maxt = int64(maxTime.(int))
	}

	newIterator := v3ioSeriesIterator{
		mint: s.set.mint, maxt: maxt, chunkTime: s.set.partition.HoursInChunk() * 3600 * 1000,
		isCyclic: s.set.partition.IsCyclic()}
	newIterator.chunks = []chunkenc.Chunk{}

	metaAttr := s.set.iter.GetField("_meta")
	if metaAttr == nil {
		s.set.logger.ErrorWith("Nil Metadata Array", "Lset", s.lset)
		s.iter = &nullSeriesIterator{}
		return
	}

	metaArray := utils.AsInt64Array(metaAttr.([]byte))
	s.set.logger.DebugWith("query meta", "array", metaArray, "attr", s.set.attrs)

	for i, attr := range s.set.attrs {
		values := s.set.iter.GetField(attr)
		chunkID := s.set.chunkIds[i]

		if values != nil && len(values.([]byte)) >= 24 && metaArray[chunkID] != 0 {
			bytes := values.([]byte)
			meta := metaArray[chunkID]
			chunk, err := chunkenc.FromBuffer(meta, bytes[16:])
			if err != nil {
				s.set.logger.ErrorWith("Error reading chunk buffer", "Lset", s.lset, "err", err)
			} else {
				newIterator.chunks = append(newIterator.chunks, chunk)
			}
		}

	}

	if len(newIterator.chunks) == 0 {
		// if there is no data, create a null iterator
		s.iter = &nullSeriesIterator{}
	} else {
		newIterator.iter = newIterator.chunks[0].Iterator()
		s.iter = &newIterator
	}
}

type v3ioSeriesIterator struct {
	mint, maxt int64 // TBD per block
	err        error
	isCyclic   bool

	chunks     []chunkenc.Chunk
	chunkIndex int
	chunkTime  int
	iter       chunkenc.Iterator
}

// advance the iterator to the specified time
func (it *v3ioSeriesIterator) Seek(t int64) bool {

	// Seek time is after the max time in object
	if t > it.maxt {
		return false
	}

	// Seek to the first valid value after t.
	if t < it.mint {
		t = it.mint
	}

	for {
		if it.iter.Next() {
			t0, _ := it.At()
			if (t > t0+int64(it.chunkTime)) || (t0 >= it.maxt && it.isCyclic) {
				// this chunk is too far behind, move to next
				if it.chunkIndex == len(it.chunks)-1 {
					return false
				}
				it.chunkIndex++
				it.iter = it.chunks[it.chunkIndex].Iterator()
			} else if t <= t0 {
				// this chunk contains data on or after t
				return true
			}
		} else {
			// End of chunk, move to next or return if last
			if it.chunkIndex == len(it.chunks)-1 {
				return false
			}
			it.chunkIndex++
			it.iter = it.chunks[it.chunkIndex].Iterator()
		}
	}

	return false
}

// move to the next iterator item
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
		if !it.isCyclic {
			return false
		}
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

// read the time & value at the current location
func (it *v3ioSeriesIterator) At() (t int64, v float64) { return it.iter.At() }

func (it *v3ioSeriesIterator) Err() error { return it.iter.Err() }

func uintToTV(data uint64, curT int64, curV float64) (int64, float64) {
	v := float64(math.Float32frombits(uint32(data)))
	t := int64(data >> 32)
	return curT + t, curV + v
}

func NewAggrSeries(set *V3ioSeriesSet, aggr aggregate.AggrType) *V3ioSeries {
	newSeries := V3ioSeries{set: set}
	lset := append(initLabels(set), utils.Label{Name: "Aggregator", Value: aggr.String()})
	newSeries.lset = lset
	newSeries.iter = &aggrSeriesIterator{set: set, aggrType: aggr, index: -1}
	return &newSeries
}

type aggrSeriesIterator struct {
	set      *V3ioSeriesSet
	aggrType aggregate.AggrType
	index    int
	err      error
}

func (s *aggrSeriesIterator) Seek(t int64) bool {
	if t <= s.set.baseTime {
		return true
	}

	if t > s.set.baseTime+int64(s.set.aggrSet.GetMaxCell())*s.set.interval {
		return false
	}

	s.index = int((t - s.set.baseTime) / s.set.interval)
	return true
}
func (s *aggrSeriesIterator) Next() bool {
	if s.index >= s.set.aggrSet.GetMaxCell() {
		return false
	}

	s.index++
	return true
}

func (s *aggrSeriesIterator) At() (t int64, v float64) {
	val := s.set.aggrSet.GetCellValue(s.aggrType, s.index)
	return s.set.aggrSet.GetCellTime(s.set.baseTime, s.index), val
}
func (s *aggrSeriesIterator) Err() error { return s.err }

type nullSeriesIterator struct {
	err error
}

func (s nullSeriesIterator) Seek(t int64) bool        { return false }
func (s nullSeriesIterator) Next() bool               { return false }
func (s nullSeriesIterator) At() (t int64, v float64) { return 0, 0 }
func (s nullSeriesIterator) Err() error               { return s.err }
