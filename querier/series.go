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
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/v3io/v3io-tsdb/chunkenc"
	"github.com/v3io/v3io-tsdb/v3ioutil"
	"math"
	"strings"
)

func NewSeries(set *seriesSet) *series {
	newSeries := series{set: set}
	newSeries.initLabels()
	newSeries.initSeriesIter()
	return &newSeries
}

type series struct {
	set  *seriesSet
	lset labels.Labels
	iter SeriesIterator
}

func (s series) Labels() labels.Labels            { return s.lset }
func (s series) Iterator() storage.SeriesIterator { return s.iter }

// SeriesIterator iterates over the data of a time series.
type SeriesIterator interface {
	// Seek advances the iterator forward to the given timestamp.
	// If there's no value exactly at t, it advances to the first value
	// after t.
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	At() (t int64, v float64)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}

// initialize the label set from _lset & name attributes
func (s *series) initLabels() {
	name := s.set.iter.GetField("_name").(string)
	lsetAttr := s.set.iter.GetField("_lset").(string)
	lset := labels.Labels{labels.Label{Name: "__name__", Value: name}}

	splitLset := strings.Split(lsetAttr, ",")
	for _, label := range splitLset {
		kv := strings.Split(label, "=")
		if len(kv) > 1 {
			lset = append(lset, labels.Label{Name: kv[0], Value: kv[1]})
		}
	}

	s.lset = lset
}

// initialize the series from value metadata & attributes
func (s *series) initSeriesIter() {

	maxt := s.set.maxt
	//maxTime := s.set.iter.GetField("_maxtime")
	//if maxTime != nil {
	//	maxt = maxTime.(int64)
	//}
	newIterator := v3ioSeriesIterator{mint: s.set.mint, maxt: maxt}
	newIterator.chunks = []chunkenc.Chunk{}

	metaAttr := s.set.iter.GetField("_meta_v")
	// TODO: handle nil
	metaArray := v3ioutil.AsInt64Array(metaAttr.([]byte))

	for i, attr := range s.set.attrs {
		values := s.set.iter.GetField(attr)
		if values != nil && len(values.([]byte)) >= 24 {
			chunkID := s.set.chunkIds[i]
			bytes := values.([]byte)
			meta := metaArray[chunkID]
			chunk, _ := chunkenc.FromBuffer(meta, bytes[16:])
			// TODO: err handle

			newIterator.chunks = append(newIterator.chunks, chunk)

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

	chunks     []chunkenc.Chunk
	chunkIndex int
	iter       chunkenc.Iterator
}

// advance the iterator to the specified time
func (it *v3ioSeriesIterator) Seek(t int64) bool {

	if t > it.maxt {
		return false
	}

	// Seek to the first valid value after t.
	if t < it.mint {
		t = it.mint
	}

	// TODO: check if T < chunk start + max hours in chunk
	//for ; it.chunks[it.i].MaxTime < t; it.i++ {
	//	if it.i == len(it.chunks)-1 {
	//		return false
	//	}
	//}
	//	it.cur = it.chunks[it.i].Chunk.Iterator()

	// TODO: can optimize to skip to the right chunk

	for i, s := range it.chunks[it.chunkIndex:] {
		if i > 0 {
			it.iter = s.Iterator()
		}
		for it.iter.Next() {
			t0, _ := it.At()
			if t0 >= t {
				return true
			}
		}
		it.chunkIndex++
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
		if t > it.maxt {
			return false
		}
		return true
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

type nullSeriesIterator struct {
	err error
}

func (s nullSeriesIterator) Seek(t int64) bool        { return false }
func (s nullSeriesIterator) Next() bool               { return false }
func (s nullSeriesIterator) At() (t int64, v float64) { return 0, 0 }
func (s nullSeriesIterator) Err() error               { return s.err }
