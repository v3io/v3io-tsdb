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

package aggregate

import (
	"fmt"
	"math"
	"strings"

	"github.com/v3io/v3io-tsdb/pkg/utils"
)

// Local cache of init arrays per aggregate type. Used to mimic memcopy and initialize data arrays with specific values
var initDataArrayCache = map[AggrType][]float64{}

type AggregateSeries struct {
	colName        string     // column name ("v" in timeseries)
	functions      []AggrType // list of aggregation functions to return (count, avg, sum, ..)
	aggrMask       AggrType   // the sum of aggregates (or between all aggregates)
	rollupTime     int64      // time per bucket (cell in the array)
	interval       int64      // requested (query) aggregation step
	buckets        int        // number of buckets in the array
	overlapWindows []int      // a list of overlapping windows (* interval), e.g. last 1hr, 6hr, 12hr, 24hr
}

func NewAggregateSeries(functions, col string, buckets int, interval, rollupTime int64, windows []int) (*AggregateSeries, error) {

	split := strings.Split(functions, ",")
	var aggrMask AggrType
	var aggrList []AggrType

	for _, s := range split {
		aggr, ok := aggrTypeString[s]
		if !ok {
			return nil, fmt.Errorf("invalid aggragator type %s", s)
		}
		aggrMask = aggrMask | aggr
		aggrList = append(aggrList, aggr)
	}

	// Always have count Aggregate by default
	if aggrMask != 0 {
		aggrMask |= aggrTypeCount
	}

	newAggregateSeries := AggregateSeries{
		aggrMask:       aggrMask,
		functions:      aggrList,
		colName:        col,
		buckets:        buckets,
		rollupTime:     rollupTime,
		interval:       interval,
		overlapWindows: windows,
	}

	return &newAggregateSeries, nil
}

func (as *AggregateSeries) CanAggregate(partitionAggr AggrType) bool {
	// keep only real aggregates
	aggrMask := 0x7f & as.aggrMask
	// make sure the DB has all the aggregates we need (on bits in the mask)
	// and that the requested interval is greater/eq to aggregate resolution and is an even divisor
	// if interval and rollup are not even divisors we need higher resolution (3x) to smooth the graph
	// when we add linear/spline graph projection we can reduce back to 1x
	return ((aggrMask & partitionAggr) == aggrMask) &&
		as.interval >= as.rollupTime && (as.interval%as.rollupTime == 0 || as.interval/as.rollupTime > 3)
}

func (as *AggregateSeries) GetAggrMask() AggrType {
	return as.aggrMask
}

func (as *AggregateSeries) GetFunctions() []AggrType {
	return as.functions
}

func (as *AggregateSeries) NumFunctions() int {
	return len(as.functions)
}

func (as *AggregateSeries) toAttrName(aggr AggrType) string {
	return "_" + as.colName + "_" + aggr.String()
}

func (as *AggregateSeries) GetAttrNames() []string {
	var names []string

	for _, aggr := range rawAggregates {
		if aggr&as.aggrMask != 0 {
			names = append(names, as.toAttrName(aggr))
		}
	}

	return names
}

// create new aggregation set from v3io aggregation array attributes
func (as *AggregateSeries) NewSetFromAttrs(
	length, start, end int, mint, maxt int64, attrs *map[string]interface{}) (*AggregateSet, error) {

	aggrArrays := map[AggrType][]uint64{}
	dataArrays := map[AggrType][]float64{}

	var maxAligned int64
	if as.overlapWindows != nil {
		length = len(as.overlapWindows)
		maxAligned = (maxt / as.interval) * as.interval
	}

	for _, aggr := range rawAggregates {
		if aggr&as.aggrMask != 0 {
			attrBlob, ok := (*attrs)[as.toAttrName(aggr)]
			if !ok {
				return nil, fmt.Errorf("aggregation attribute %s was not found", as.toAttrName(aggr))
			}
			aggrArrays[aggr] = utils.AsInt64Array(attrBlob.([]byte))

			dataArrays[aggr] = make([]float64, length, length)
			copy(dataArrays[aggr], getOrCreateInitDataArray(aggr, length))
		}
	}

	aggrSet := AggregateSet{length: length, interval: as.interval, overlapWin: as.overlapWindows}
	aggrSet.dataArrays = dataArrays

	arrayIndex := start
	i := 0

	for arrayIndex != end {

		if as.overlapWindows == nil {

			// standard aggregates (evenly spaced intervals)
			cellIndex := int((int64(i) * as.rollupTime) / as.interval)
			for aggr, array := range aggrArrays {
				aggrSet.mergeArrayCell(aggr, cellIndex, array[arrayIndex])
			}
		} else {

			// overlapping time windows (last 1hr, 6hr, ..)
			t := mint + (int64(i) * as.rollupTime)
			if t < maxAligned {
				for i, win := range as.overlapWindows {
					if t > maxAligned-int64(win)*as.interval {
						for aggr, array := range aggrArrays {
							aggrSet.mergeArrayCell(aggr, i, array[arrayIndex])
						}
					}
				}
			}

		}

		i++
		arrayIndex = (arrayIndex + 1) % (as.buckets + 1)
	}

	return &aggrSet, nil
}

// prepare new aggregation set from v3io raw chunk attributes (in case there are no aggregation arrays)
func (as *AggregateSeries) NewSetFromChunks(length int) *AggregateSet {

	if as.overlapWindows != nil {
		length = len(as.overlapWindows)
	}

	newAggregateSet := AggregateSet{length: length, interval: as.interval, overlapWin: as.overlapWindows}
	dataArrays := map[AggrType][]float64{}

	for _, aggr := range rawAggregates {
		if aggr&as.aggrMask != 0 {
			dataArrays[aggr] = make([]float64, length, length) // TODO: len/capacity & reuse (pool)
			initArray := getOrCreateInitDataArray(aggr, length)
			copy(dataArrays[aggr], initArray)
		}
	}

	newAggregateSet.dataArrays = dataArrays
	return &newAggregateSet
}

type AggregateSet struct {
	dataArrays map[AggrType][]float64
	length     int
	maxCell    int
	baseTime   int64
	interval   int64
	overlapWin []int
}

func (as *AggregateSet) GetMaxCell() int {
	return as.maxCell
}

// append the value to a cell in all relevant aggregation arrays
func (as *AggregateSet) AppendAllCells(cell int, val float64) {

	if !isValidCell(cell, as) {
		return
	}

	if cell > as.maxCell {
		as.maxCell = cell
	}

	for aggr := range as.dataArrays {
		as.updateCell(aggr, cell, val)
	}
}

// append/merge server aggregation values into aggregation per requested interval/step
// if the requested step interval is higher than stored interval we need to collapse multiple cells to one
func (as *AggregateSet) mergeArrayCell(aggr AggrType, cell int, val uint64) {

	if cell >= as.length {
		return
	}

	if cell > as.maxCell {
		as.maxCell = cell
	}

	if aggr == aggrTypeCount {
		as.dataArrays[aggr][cell] += float64(val)
	} else {
		float := math.Float64frombits(val)
		// When getting already aggregated sqr aggregate we just need to sum.
		if aggr == aggrTypeSqr {
			as.dataArrays[aggr][cell] += float
		} else {
			as.updateCell(aggr, cell, float)
		}
	}
}

func isValidCell(cellIndex int, aSet *AggregateSet) bool {
	return cellIndex >= 0 &&
		cellIndex < aSet.length
}

// function specific aggregation
func (as *AggregateSet) updateCell(aggr AggrType, cell int, val float64) {

	if !isValidCell(cell, as) {
		return
	}

	cellValue := as.dataArrays[aggr][cell]
	switch aggr {
	case aggrTypeCount:
		as.dataArrays[aggr][cell] += 1
	case aggrTypeSum:
		as.dataArrays[aggr][cell] += val
	case aggrTypeSqr:
		as.dataArrays[aggr][cell] += val * val
	case aggrTypeMin:
		if val < cellValue {
			as.dataArrays[aggr][cell] = val
		}
	case aggrTypeMax:
		if val > cellValue {
			as.dataArrays[aggr][cell] = val
		}
	case aggrTypeLast:
		as.dataArrays[aggr][cell] = val
	}
}

// return the value per aggregate or complex function
func (as *AggregateSet) GetCellValue(aggr AggrType, cell int) (float64, bool) {

	if !isValidCell(cell, as) {
		return math.NaN(), false
	}

	dependsOnSum := aggr == aggrTypeStddev || aggr == aggrTypeStdvar || aggr == aggrTypeAvg
	dependsOnSqr := aggr == aggrTypeStddev || aggr == aggrTypeStdvar
	dependsOnLast := aggr == aggrTypeLast || aggr == aggrTypeRate

	// return undefined result one dependant fields is missing
	if (dependsOnSum && utils.IsUndefined(as.dataArrays[aggrTypeSum][cell])) ||
		(dependsOnSqr && utils.IsUndefined(as.dataArrays[aggrTypeSqr][cell]) ||
			(dependsOnLast && utils.IsUndefined(as.dataArrays[aggrTypeLast][cell]))) {
		return math.NaN(), false
	}

	// if no samples in this bucket the result is undefined
	var cnt float64
	if dependsOnSum {
		cnt = as.dataArrays[aggrTypeCount][cell]
		if cnt == 0 {
			return math.NaN(), false
		}
	}

	switch aggr {
	case aggrTypeAvg:
		return as.dataArrays[aggrTypeSum][cell] / cnt, true
	case aggrTypeStddev:
		sum := as.dataArrays[aggrTypeSum][cell]
		sqr := as.dataArrays[aggrTypeSqr][cell]
		return math.Sqrt((cnt*sqr - sum*sum) / (cnt * (cnt - 1))), true
	case aggrTypeStdvar:
		sum := as.dataArrays[aggrTypeSum][cell]
		sqr := as.dataArrays[aggrTypeSqr][cell]
		return (cnt*sqr - sum*sum) / (cnt * (cnt - 1)), true
	case aggrTypeRate:
		if cell == 0 {
			return math.NaN(), false
		}
		// TODO: need to clarify the meaning of this type of aggregation. IMHO, rate has meaning for monotonic counters only
		last := as.dataArrays[aggrTypeLast][cell-1]
		this := as.dataArrays[aggrTypeLast][cell]
		return (this - last) / float64(as.interval/1000), true // rate per sec
	default:
		return as.dataArrays[aggr][cell], true
	}
}

// get the time per aggregate cell
func (as *AggregateSet) GetCellTime(base int64, index int) int64 {
	if as.overlapWin == nil {
		return base + int64(index)*as.interval
	}

	if index >= len(as.overlapWin) {
		return base
	}

	return base - int64(as.overlapWin[index])*as.interval
}

func (as *AggregateSet) Clear() {
	as.maxCell = 0
	for aggr := range as.dataArrays {
		initArray := getOrCreateInitDataArray(aggr, len(as.dataArrays[0]))
		copy(as.dataArrays[aggr], initArray)
	}
}

// Check if cell has data. Assumes that count is always present
func (as *AggregateSet) HasData(cell int) bool {
	return as.dataArrays[aggrTypeCount][cell] > 0
}

func getOrCreateInitDataArray(aggrType AggrType, length int) []float64 {
	// Create once or override if required size is greater than existing array
	if initDataArrayCache[aggrType] == nil || len(initDataArrayCache[aggrType]) < length {
		initDataArrayCache[aggrType] = createInitDataArray(aggrType, length)
	}
	return initDataArrayCache[aggrType]
}

func createInitDataArray(aggrType AggrType, length int) []float64 {
	// Prepare "clean" array for fastest reset of "uninitialized" data arrays
	resultArray := make([]float64, length, length)

	var initWith float64
	switch aggrType {
	case aggrTypeMin:
		initWith = math.Inf(1)
	case aggrTypeMax:
		initWith = math.Inf(-1)
	default:
		// NOP - default is 0
	}

	for i := range resultArray {
		resultArray[i] = initWith
	}

	return resultArray
}
