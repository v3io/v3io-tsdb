package aggregate

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math"
)

type RawAggregatedSeries struct {
	params AggregationParams

	length                       int
	queryStartTime, queryEndTime int64
	aggregates                   map[AggrType][]float64
}

func NewRawAggregatedSeries(length int, mint, maxt int64, params AggregationParams) *RawAggregatedSeries {

	series := &RawAggregatedSeries{params: params}
	series.queryStartTime, series.queryEndTime = mint, maxt
	series.length = length

	// Create the final aggregations array with default values.
	series.aggregates = map[AggrType][]float64{}
	for _, aggr := range rawAggregates {
		if aggr&params.aggrMask != 0 {
			series.aggregates[aggr] = make([]float64, length, length)
			copy(series.aggregates[aggr], GetOrCreateInitDataArray(aggr, length))
		}
	}

	return series
}

func (as *RawAggregatedSeries) AggregateData(fields map[string]interface{}, mint int64) {
	for aggr := range as.aggregates {
		array, ok := fields[as.toAttrName(aggr)]
		if !ok {
			// TODO return error
		}
		arrayData := utils.AsInt64Array(array.([]byte))
		for i, val := range arrayData {
			currentValueTime := mint + int64(i+1)*as.params.RollupTime
			currentCell := (currentValueTime - as.queryStartTime) / as.params.Interval
			as.updateCell(aggr, int(currentCell), val)
		}
	}
}

func (as *RawAggregatedSeries) GetAggregate(aggr AggrType) []float64 {
	count := as.aggregates[aggrTypeCount]
	switch aggr {
	case aggrTypeAvg:
		avgAggregate := make([]float64, as.length)
		sum := as.aggregates[aggrTypeSum]
		for i := 0; i < as.length; i++ {
			if count[i] > 0 {
				avgAggregate[i] = sum[i] / count[i]
			}
		}
		return avgAggregate
	case aggrTypeStddev:
		sum := as.aggregates[aggrTypeSum]
		sqr := as.aggregates[aggrTypeSqr]
		stddevAggregate := make([]float64, as.length)
		for i := 0; i < as.length; i++ {
			if count[i] > 0 {
				stddevAggregate[i] = math.Sqrt((count[i]*sqr[i] - sum[i]*sum[i]) / (count[i] * (count[i] - 1)))
			}
		}
		return stddevAggregate
	case aggrTypeStdvar:
		sum := as.aggregates[aggrTypeSum]
		sqr := as.aggregates[aggrTypeSqr]
		stdvarAggregate := make([]float64, as.length)
		for i := 0; i < as.length; i++ {
			if count[i] > 0 {
				stdvarAggregate[i] = (count[i]*sqr[i] - sum[i]*sum[i]) / (count[i] * (count[i] - 1))
			}
		}
		return stdvarAggregate
	case aggrTypeRate:
		// TODO: need to clarify the meaning of this type of aggregation. IMHO, rate has meaning for monotonic counters only
		last := as.aggregates[aggrTypeLast]
		rateAggregate := make([]float64, as.length)
		rateAggregate[0] = math.NaN()
		for i := 1; i < as.length; i++ {
			rateAggregate[i] = (last[i] - last[i-1]) / float64(as.params.Interval/1000) // rate per sec
		}
		return rateAggregate
	default:
		return as.aggregates[aggr]
	}
}

func (as *RawAggregatedSeries) updateCell(aggr AggrType, cell int, val uint64) {

	if !as.isValidCell(cell) {
		return
	}

	// Count is saved as int array while other aggregations are saved as float.
	if aggr == aggrTypeCount {
		as.aggregates[aggr][cell] += float64(val)
	} else {
		floatVal := math.Float64frombits(val)
		cellValue := as.aggregates[aggr][cell]
		switch aggr {
		case aggrTypeSum:
			as.aggregates[aggr][cell] += floatVal
		case aggrTypeSqr:
			as.aggregates[aggr][cell] += floatVal
		case aggrTypeMin:
			if floatVal < cellValue {
				as.aggregates[aggr][cell] = floatVal
			}
		case aggrTypeMax:
			if floatVal > cellValue {
				as.aggregates[aggr][cell] = floatVal
			}
		case aggrTypeLast:
			as.aggregates[aggr][cell] = floatVal
		}
	}
}

func (as *RawAggregatedSeries) toAttrName(aggr AggrType) string {
	return "_v_" + aggr.String()
}

func (as *RawAggregatedSeries) isValidCell(cellIndex int) bool {
	return cellIndex >= 0 &&
		cellIndex < as.length
}

func ToAttrName(aggr AggrType) string {
	return "_v_" + aggr.String()
}

func GetServerAggregationsFunction(aggr AggrType) (func(interface{}, interface{}) interface{}, error) {
	switch aggr {
	case aggrTypeCount:
		return func(old, next interface{}) interface{} {
			if old == nil {
				return next
			}
			return old.(float64) + next.(float64)
		}, nil
	case aggrTypeSum:
		return func(old, next interface{}) interface{} {
			if old == nil {
				return next
			}
			return old.(float64) + next.(float64)
		}, nil
	case aggrTypeSqr:
		return func(old, next interface{}) interface{} {
			if old == nil {
				return next
			}
			return old.(float64) + next.(float64)
		}, nil
	case aggrTypeMin:
		return func(old, next interface{}) interface{} {
			if old == nil {
				return next
			}
			return math.Min(old.(float64), next.(float64))
		}, nil
	case aggrTypeMax:
		return func(old, next interface{}) interface{} {
			if old == nil {
				return next
			}
			return math.Max(old.(float64), next.(float64))
		}, nil
	case aggrTypeLast:
		return func(_, next interface{}) interface{} {
			return next
		}, nil
	default:
		return nil, fmt.Errorf("cannot aggregate %v", aggrToString[aggr])
	}
}

func GetServerVirtualAggregationFunction(aggr AggrType) (func([]float64) float64, error) {
	switch aggr {
	case aggrTypeAvg:
		return func(data []float64) float64 {
			count := data[0]
			sum := data[1]
			return sum / count
		}, nil
	case aggrTypeStddev:
		return func(data []float64) float64 {
			count := data[0]
			sum := data[1]
			sqr := data[2]
			return math.Sqrt((count*sqr - sum*sum) / (count * (count - 1)))
		}, nil
	case aggrTypeStdvar:
		return func(data []float64) float64 {
			count := data[0]
			sum := data[1]
			sqr := data[2]
			return (count*sqr - sum*sum) / (count * (count - 1))
		}, nil
	default:
		return nil, fmt.Errorf("cannot aggregate %v", aggrToString[aggr])
	}
}

func GetClientAggregationsFunction(aggr AggrType) (func(interface{}, interface{}) interface{}, error) {
	switch aggr {
	case aggrTypeCount:
		return func(old, next interface{}) interface{} {
			if old == nil {
				return 1.0
			}
			return old.(float64) + 1.0
		}, nil
	case aggrTypeSqr:
		return func(old, next interface{}) interface{} {
			if old == nil {
				return next.(float64) * next.(float64)
			}
			return old.(float64) + next.(float64)*next.(float64)
		}, nil
	default:
		return GetServerAggregationsFunction(aggr)
	}
}

func GetDependantAggregates(aggr AggrType) []AggrType {
	var aggregates []AggrType
	for _, rawAggr := range rawAggregates {
		if aggr&rawAggr == rawAggr {
			aggregates = append(aggregates, rawAggr)
		}
	}
	return aggregates
}
