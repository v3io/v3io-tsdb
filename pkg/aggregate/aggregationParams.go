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
	"strings"
)

type AggregationParams struct {
	colName                       string   // column name ("v" in timeseries)
	aggrMask                      AggrType // the sum of aggregates (or between all aggregates)
	rollupTime                    int64    // time per bucket (cell in the array)
	Interval                      int64    // requested (query) aggregation step
	buckets                       int      // number of buckets in the array
	overlapWindows                []int    // a list of overlapping windows (* interval), e.g. last 1hr, 6hr, 12hr, 24hr
	aggregationWindow             int64    // a time window on which to calculate the aggregation per Interval
	disableClientAggregation      bool
	useServerAggregateCoefficient int
}

func NewAggregationParams(functions, col string,
	buckets int,
	interval, aggregationWindow, rollupTime int64,
	windows []int,
	disableClientAggregation bool,
	useServerAggregateCoefficient int) (*AggregationParams, error) {

	aggregatesList := strings.Split(functions, ",")
	aggrMask, _, err := AggregatesFromStringListWithCount(aggregatesList)
	if err != nil {
		return nil, err
	}

	newAggregateSeries := AggregationParams{
		aggrMask:                      aggrMask,
		colName:                       col,
		buckets:                       buckets,
		rollupTime:                    rollupTime,
		aggregationWindow:             aggregationWindow,
		Interval:                      interval,
		overlapWindows:                windows,
		disableClientAggregation:      disableClientAggregation,
		useServerAggregateCoefficient: useServerAggregateCoefficient,
	}

	return &newAggregateSeries, nil
}

func (as *AggregationParams) CanAggregate(partitionAggr AggrType) bool {
	// Get only the raw aggregates from what the user requested
	aggrMask := rawAggregatesMask & as.aggrMask
	// make sure the DB has all the aggregates we need (on bits in the mask)
	// and that the requested interval is greater/eq to aggregate resolution and is an even divisor
	// if interval and rollup are not even divisors we need higher resolution (3x) to smooth the graph
	// when we add linear/spline graph projection we can reduce back to 1x
	return ((aggrMask & partitionAggr) == aggrMask) &&
		(as.Interval/as.rollupTime > int64(as.useServerAggregateCoefficient) || (as.Interval == as.rollupTime && as.disableClientAggregation)) &&
		(as.aggregationWindow == 0 || as.aggregationWindow >= as.rollupTime)
}

func (as *AggregationParams) GetAggrMask() AggrType {
	return as.aggrMask
}

func (as *AggregationParams) GetRollupTime() int64 {
	return as.rollupTime
}

func (as *AggregationParams) GetAggregationWindow() int64 {
	return as.aggregationWindow
}

func (as *AggregationParams) HasAggregationWindow() bool {
	return as.aggregationWindow > 0
}

func (as *AggregationParams) toAttrName(aggr AggrType) string {
	return fmt.Sprintf("_%v_%v", as.colName, aggr.String())
}

func (as *AggregationParams) GetAttrNames() []string {
	var names []string

	for _, aggr := range rawAggregates {
		if aggr&as.aggrMask != 0 {
			names = append(names, as.toAttrName(aggr))
		}
	}

	return names
}
