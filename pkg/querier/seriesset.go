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
)

// holds the query result set
type V3ioSeriesSet struct {
	err        error
	logger     logger.Logger
	partition  *partmgr.DBPartition
	iter       utils.ItemsCursor
	mint, maxt int64
	attrs      []string
	chunk0Time int64

	interval     int64
	nullSeries   bool
	overlapWin   []int
	aggrSeries   *aggregate.AggregateSeries
	aggrIdx      int
	canAggregate bool
	currSeries   utils.Series
	aggrSet      *aggregate.AggregateSet
	noAggrLbl    bool
	baseTime     int64
}

// Get relevant items and attributes from the TSDB and create an iterator
// TODO: get items per partition + merge, per partition calc attrs
func (s *V3ioSeriesSet) getItems(partition *partmgr.DBPartition, name, filter string, container *v3io.Container, workers int) error {

	path := partition.GetTablePath()
	shardingKeys := []string{}
	if name != "" {
		shardingKeys = partition.GetShardingKeys(name)
	}
	attrs := []string{config.LabelSetAttrName, config.OutOfOrderAttrName, "_name", config.MaxTimeAttrName}

	if s.aggrSeries != nil && s.canAggregate {
		s.attrs = s.aggrSeries.GetAttrNames()
	} else {
		s.attrs, s.chunk0Time = s.partition.Range2Attrs("v", s.mint, s.maxt)
	}
	attrs = append(attrs, s.attrs...)

	s.logger.DebugWith("Select - GetItems", "path", path, "attr", attrs, "filter", filter, "name", name)
	input := v3io.GetItemsInput{Path: path, AttributeNames: attrs, Filter: filter, ShardingKey: name}
	iter, err := utils.NewAsyncItemsCursor(container, &input, workers, shardingKeys, s.logger)
	if err != nil {
		return err
	}

	s.iter = iter
	return nil

}

// Advance to the next series
func (s *V3ioSeriesSet) Next() bool {

	// Create a raw-chunks series (not aggregated)
	if s.aggrSeries == nil {
		if s.iter.Next() {
			s.currSeries = NewSeries(s)
			return true
		}
		return false
	}

	// Create multiple aggregation series (one per aggregation function).
	// The index is initialized as numfunc-1 (so the first +1 and modulo will equal 0).
	if s.aggrIdx == s.aggrSeries.NumFunctions()-1 {
		// If there are no more items (from GetItems cursor), return with EOF
		if !s.iter.Next() {
			return false
		}

		s.nullSeries = false

		if s.canAggregate {

			// Create a series from aggregation arrays (in the TSDB table) if
			// the partition stores the desired aggregates
			maxtUpdate := s.maxt
			maxTime := s.iter.GetField(config.MaxTimeAttrName)
			if maxTime != nil && int64(maxTime.(int)) < s.maxt {
				maxtUpdate = int64(maxTime.(int))
			}

			start := s.partition.Time2Bucket(s.mint)
			end := s.partition.Time2Bucket(s.maxt+s.interval) + 1

			// Calculate the length of the returned array: time-range/interval + 2
			length := int((maxtUpdate-s.mint)/s.interval) + 2

			if s.overlapWin != nil {
				s.baseTime = s.maxt
			} else {
				s.baseTime = s.mint
			}

			if length > 0 {
				attrs := s.iter.GetFields()
				aggrSet, err := s.aggrSeries.NewSetFromAttrs(length, start, end, s.mint, s.maxt, &attrs)
				if err != nil {
					s.err = err
					return false
				}

				s.aggrSet = aggrSet
			} else {
				s.nullSeries = true
			}

		} else {

			// Create a series from raw chunks
			s.currSeries = NewSeries(s)

			// Calculate the number of cells: (maxt-mint)/interval + 1
			numCells := (s.maxt-s.mint)/s.interval + 1

			s.aggrSet = s.aggrSeries.NewSetFromChunks(int(numCells))
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

// Convert raw chunks to a fixed-interval aggregate
func (s *V3ioSeriesSet) chunks2IntervalAggregates() {

	iter := s.currSeries.Iterator()
	if iter.Next() {

		s.baseTime = s.mint

		for {
			t, v := iter.At()
			s.aggrSet.AppendAllCells(int((t-s.baseTime)/s.interval), v)
			if !iter.Next() {
				break
			}
		}
	}

	if iter.Err() != nil {
		s.err = iter.Err()
		return
	}
}

// Convert chunks to an overlapping-windows aggregate
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

// Return the current error
func (s *V3ioSeriesSet) Err() error {
	if s.iter.Err() != nil {
		return s.iter.Err()
	}
	return s.err
}

// Return a series iterator
func (s *V3ioSeriesSet) At() utils.Series {
	if s.aggrSeries == nil {
		return s.currSeries
	}

	return NewAggrSeries(s, s.aggrSeries.GetFunctions()[s.aggrIdx])
}
