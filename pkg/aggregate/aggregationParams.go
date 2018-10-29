package aggregate

import (
	"fmt"
	"strings"
)

type AggregationParams struct {
	colName        string     // column name ("v" in timeseries)
	functions      []AggrType // list of aggregation functions to return (count, avg, sum, ..)
	aggrMask       AggrType   // the sum of aggregates (or between all aggregates)
	RollupTime     int64      // time per bucket (cell in the array)
	Interval       int64      // requested (query) aggregation step
	buckets        int        // number of buckets in the array
	overlapWindows []int      // a list of overlapping windows (* interval), e.g. last 1hr, 6hr, 12hr, 24hr
}

func NewAggregationParams(functions, col string, buckets int, interval, rollupTime int64, windows []int) (*AggregationParams, error) {

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

	newAggregateSeries := AggregationParams{
		aggrMask:       aggrMask,
		functions:      aggrList,
		colName:        col,
		buckets:        buckets,
		RollupTime:     rollupTime,
		Interval:       interval,
		overlapWindows: windows,
	}

	return &newAggregateSeries, nil
}

func (as *AggregationParams) CanAggregate(partitionAggr AggrType) bool {
	// keep only real aggregates
	aggrMask := 0x7f & as.aggrMask
	// make sure the DB has all the aggregates we need (on bits in the mask)
	// and that the requested interval is greater/eq to aggregate resolution and is an even divisor
	// if interval and rollup are not even divisors we need higher resolution (3x) to smooth the graph
	// when we add linear/spline graph projection we can reduce back to 1x
	return ((aggrMask & partitionAggr) == aggrMask) &&
		as.Interval >= as.RollupTime && (as.Interval%as.RollupTime == 0 || as.Interval/as.RollupTime > 3)
}

func (as *AggregationParams) GetAggrMask() AggrType {
	return as.aggrMask
}

func (as *AggregationParams) GetFunctions() []AggrType {
	return as.functions
}

func (as *AggregationParams) NumFunctions() int {
	return len(as.functions)
}

func (as *AggregationParams) toAttrName(aggr AggrType) string {
	return "_" + as.colName + "_" + aggr.String()
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
