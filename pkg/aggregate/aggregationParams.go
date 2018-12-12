package aggregate

import (
	"fmt"
	"strings"
)

type AggregationParams struct {
	colName        string   // column name ("v" in timeseries)
	aggrMask       AggrType // the sum of aggregates (or between all aggregates)
	rollupTime     int64    // time per bucket (cell in the array)
	Interval       int64    // requested (query) aggregation step
	buckets        int      // number of buckets in the array
	overlapWindows []int    // a list of overlapping windows (* interval), e.g. last 1hr, 6hr, 12hr, 24hr
}

func NewAggregationParams(functions, col string, buckets int, interval, rollupTime int64, windows []int) (*AggregationParams, error) {

	aggregatesList := strings.Split(functions, ",")
	aggrMask, _, err := AggregatesFromStringListWithCount(aggregatesList)
	if err != nil {
		return nil, err
	}

	newAggregateSeries := AggregationParams{
		aggrMask:       aggrMask,
		colName:        col,
		buckets:        buckets,
		rollupTime:     rollupTime,
		Interval:       interval,
		overlapWindows: windows,
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
		as.Interval >= as.rollupTime && (as.Interval%as.rollupTime == 0 || as.Interval/as.rollupTime > 3)
}

func (as *AggregationParams) GetAggrMask() AggrType {
	return as.aggrMask
}

func (as *AggregationParams) GetRollupTime() int64 {
	return as.rollupTime
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
