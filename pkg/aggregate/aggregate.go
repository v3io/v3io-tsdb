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

type AggrType uint16

// aggregation functions
const (
	aggrTypeCount AggrType = 1
	aggrTypeSum   AggrType = 2
	aggrTypeSqr   AggrType = 4
	aggrTypeMax   AggrType = 8
	aggrTypeMin   AggrType = 16
	aggrTypeLast  AggrType = 32

	// derived aggregators
	aggrTypeAvg    AggrType = aggrTypeCount | aggrTypeSum
	aggrTypeRate   AggrType = aggrTypeLast | 0x8000
	aggrTypeStddev AggrType = aggrTypeCount | aggrTypeSum | aggrTypeSqr
	aggrTypeStdvar AggrType = aggrTypeCount | aggrTypeSum | aggrTypeSqr | 0x8000
	aggrTypeAll    AggrType = 0xffff
)

var rawAggregators = []AggrType{aggrTypeCount, aggrTypeSum, aggrTypeSqr, aggrTypeMax, aggrTypeMin, aggrTypeLast}

var aggrTypeString = map[string]AggrType{
	"count": aggrTypeCount, "sum": aggrTypeSum, "sqr": aggrTypeSqr, "max": aggrTypeMax, "min": aggrTypeMin,
	"last": aggrTypeLast, "avg": aggrTypeAvg, "rate": aggrTypeRate,
	"stddev": aggrTypeStddev, "stdvar": aggrTypeStdvar, "*": aggrTypeAll}

var aggrToString = map[AggrType]string{
	aggrTypeCount: "count", aggrTypeSum: "sum", aggrTypeSqr: "sqr", aggrTypeMin: "min", aggrTypeMax: "max",
	aggrTypeLast: "last", aggrTypeAvg: "avg", aggrTypeRate: "rate",
	aggrTypeStddev: "stddev", aggrTypeStdvar: "stdvar", aggrTypeAll: "*",
}

func (a AggrType) String() string { return aggrToString[a] }

// convert comma separated string to aggregator mask
func AggrsFromString(list string) (AggrType, error) {
	split := strings.Split(list, ",")
	var aggrList AggrType
	for _, s := range split {
		aggr, ok := aggrTypeString[s]
		if !ok {
			return aggrList, fmt.Errorf("Invalid aggragator type %s", s)
		}
		aggrList = aggrList | aggr
	}
	return aggrList, nil
}

// create list of aggregator objects from aggregator mask
func NewAggregatorList(aggrType AggrType) *AggregatorList {
	list := AggregatorList{}
	if (aggrType & aggrTypeCount) != 0 {
		list = append(list, &CountAggregator{})
	}
	if (aggrType & aggrTypeSum) != 0 {
		list = append(list, &SumAggregator{FloatAggregator{attr: "sum"}})
	}
	if (aggrType & aggrTypeSqr) != 0 {
		list = append(list, &SqrAggregator{FloatAggregator{attr: "sqr"}})
	}
	if (aggrType & aggrTypeMin) != 0 {
		list = append(list, &MinAggregator{FloatAggregator{attr: "min"}})
	}
	if (aggrType & aggrTypeMax) != 0 {
		list = append(list, &MaxAggregator{FloatAggregator{attr: "max"}})
	}
	if (aggrType & aggrTypeLast) != 0 {
		list = append(list, &LastAggregator{FloatAggregator{attr: "last"}, 0})
	}
	return &list
}

// list of aggregators
type AggregatorList []Aggregator

// append value to all aggregators
func (a AggregatorList) Aggregate(t int64, val interface{}) {
	v := val.(float64)
	for _, aggr := range a {
		aggr.Aggregate(t, v)
	}
}

// return update expression for aggregators
func (a AggregatorList) UpdateExpr(col string, bucket int) string {
	expr := ""
	for _, aggr := range a {
		expr = expr + aggr.UpdateExpr(col, bucket)
	}
	return expr
}

// return set (first value) or update expression for aggregators
func (a AggregatorList) SetOrUpdateExpr(col string, bucket int, isNew bool) string {
	if isNew {
		return a.SetExpr(col, bucket)
	}
	return a.UpdateExpr(col, bucket)
}

func (a AggregatorList) SetExpr(col string, bucket int) string {
	expr := ""
	for _, aggr := range a {
		expr = expr + aggr.SetExpr(col, bucket)
	}
	return expr
}

// return array init expression
func (a AggregatorList) InitExpr(col string, buckets int) string {
	expr := ""
	for _, aggr := range a {
		expr = expr + aggr.InitExpr(col, buckets)
	}
	return expr
}

// clear all aggregators
func (a AggregatorList) Clear() {
	for _, aggr := range a {
		aggr.Clear()
	}
}
