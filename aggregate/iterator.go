package aggregate

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/v3ioutil"
	"math"
	"strings"
)

type AggregateSeries struct {
	//partition  *partmgr.DBPartition
	colName    string
	functions  []AggrType
	aggregates AggrType
	dataArrays map[AggrType][]float64
	mint, maxt int64
	interval   int64
	baseTime   int64
}

func NewAggregateSeries(functions, col string) (*AggregateSeries, error) {

	if functions == "" {
		return nil, nil
	}

	split := strings.Split(functions, ",")
	var aggrSum AggrType
	aggrList := []AggrType{}
	for _, s := range split {
		aggr, ok := aggrTypeString[s]
		if !ok {
			return nil, fmt.Errorf("Invalid aggragator type %s", s)
		}
		aggrSum = aggrSum | aggr
		aggrList = append(aggrList, aggr)
	}

	newAggregateSeries := AggregateSeries{}
	newAggregateSeries.aggregates = aggrSum
	newAggregateSeries.functions = aggrList
	newAggregateSeries.colName = col

	return &newAggregateSeries, nil
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
	names := []string{}

	for _, aggr := range rawAggregators {
		if aggr&as.aggregates != 0 {
			names = append(names, as.toAttrName(aggr))
		}
	}

	return names
}

func (as *AggregateSeries) LoadAttrs(attrs *map[string]interface{}) error {

	aggrArrays := map[AggrType][]uint64{}

	for _, aggr := range rawAggregators {
		if aggr&as.aggregates != 0 {
			attrBlob, ok := (*attrs)[as.toAttrName(aggr)]
			if !ok {
				return fmt.Errorf("Aggregation Attribute %s was not found", as.toAttrName(aggr))
			}
			aggrArrays[aggr] = v3ioutil.AsInt64Array(attrBlob.([]byte))
		}
	}

	// figure out the array start-end based on min/max (can be cyclic)

	// merge multiple cells (per aggr) into dataArrays based on desired interval vs array (partition) interval

	return nil
}

type AggregateSet struct {
	as         *AggregateSeries
	dataArrays map[AggrType][]float64
	length     int
	maxCell    int
}

func (as *AggregateSeries) NewAggregateSet(length int) *AggregateSet {
	newAggregateSet := AggregateSet{as: as, length: length}
	dataArrays := map[AggrType][]float64{}

	for _, aggr := range rawAggregators {
		if aggr&as.aggregates != 0 {
			dataArrays[aggr] = make([]float64, length, length) // TODO: len/capacity & reuse (pool)
		}
	}

	newAggregateSet.dataArrays = dataArrays
	return &newAggregateSet
}

func (as *AggregateSet) GetMaxCell() int {
	return as.maxCell
}

func (as *AggregateSet) AppendCell(cell int, val float64) {

	if cell > as.length {
		return
	}

	if cell > as.maxCell {
		as.maxCell = cell
	}

	for aggr, _ := range as.dataArrays {
		switch aggr {
		case aggrTypeCount:
			as.dataArrays[aggr][cell] += 1
		case aggrTypeSum:
			as.dataArrays[aggr][cell] += val
		case aggrTypeSqr:
			as.dataArrays[aggr][cell] += val * val
		case aggrTypeMin:
			if val < as.dataArrays[aggr][cell] {
				as.dataArrays[aggr][cell] = val
			}
		case aggrTypeMax:
			if val > as.dataArrays[aggr][cell] {
				as.dataArrays[aggr][cell] = val
			}
		}
	}
}

func (as *AggregateSet) GetCellValue(aggr AggrType, cell int) float64 {

	if cell > as.maxCell {
		return math.NaN()
	}

	if cell > as.length {
		return 0
	}

	switch aggr {
	case aggrTypeAvg:
		return as.dataArrays[aggrTypeSum][cell] / as.dataArrays[aggrTypeCount][cell]
	case aggrTypeStddev:
		cnt := as.dataArrays[aggrTypeCount][cell]
		sum := as.dataArrays[aggrTypeSum][cell]
		sqr := as.dataArrays[aggrTypeSqr][cell]
		return math.Sqrt((cnt*sqr - sum*sum) / (cnt * (cnt - 1)))
	case aggrTypeStdvar:
		cnt := as.dataArrays[aggrTypeCount][cell]
		sum := as.dataArrays[aggrTypeSum][cell]
		sqr := as.dataArrays[aggrTypeSqr][cell]
		return (cnt*sqr - sum*sum) / (cnt * (cnt - 1))
	default:
		return as.dataArrays[aggr][cell]
	}

}

func (as *AggregateSet) Clear() {
	as.maxCell = 0
	for aggr, _ := range as.dataArrays {
		as.dataArrays[aggr] = as.dataArrays[aggr][:0]
	}
}
