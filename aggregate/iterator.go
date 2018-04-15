package aggregate

import (
	"fmt"
	"github.com/v3io/v3io-tsdb/v3ioutil"
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
