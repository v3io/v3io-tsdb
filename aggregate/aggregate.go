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

type AggrType uint8

const (
	aggrTypeCount AggrType = 1
	aggrTypeSum   AggrType = 2
	aggrTypeSqr   AggrType = 4
	aggrTypeMax   AggrType = 8
	aggrTypeMin   AggrType = 16

	aggrTypeAvg    AggrType = aggrTypeCount | aggrTypeSum
	aggrTypeStddev AggrType = aggrTypeCount | aggrTypeSum | aggrTypeSqr
)

var aggrTypeString = map[string]AggrType{
	"cnt": aggrTypeCount, "sum": aggrTypeSum, "sqr": aggrTypeSqr, "min": aggrTypeMin,
	"max": aggrTypeMax, "avg": aggrTypeAvg, "stddev": aggrTypeStddev}

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

func NewAggregatorList(aggrType AggrType) *AggregatorList {
	list := AggregatorList{}
	if (aggrType & aggrTypeCount) != 0 {
		list = append(list, &CountAggregator{})
	}
	if (aggrType & aggrTypeSum) != 0 {
		list = append(list, &SumAggregator{})
	}
	return &list
}

type AggregatorList []Aggregator

func (a AggregatorList) Aggregate(v float64) {
	for _, aggr := range a {
		aggr.Aggregate(v)
	}
}

func (a AggregatorList) UpdateExpr(col string, bucket int) string {
	expr := ""
	for _, aggr := range a {
		expr = expr + aggr.UpdateExpr(col, bucket)
	}
	return expr
}

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

func (a AggregatorList) InitExpr(col string, buckets int) string {
	expr := ""
	for _, aggr := range a {
		expr = expr + aggr.InitExpr(col, buckets)
	}
	return expr
}

func (a AggregatorList) Clear() {
	for _, aggr := range a {
		aggr.Clear()
	}
}

type Aggregator interface {
	Aggregate(v float64)
	Clear()
	GetType() AggrType
	UpdateExpr(col string, bucket int) string
	SetExpr(col string, bucket int) string
	InitExpr(col string, buckets int) string
}

type CountAggregator struct {
	count int
}

func (a *CountAggregator) Aggregate(v float64) { a.count++ }
func (a *CountAggregator) Clear()              { a.count = 0 }
func (a *CountAggregator) GetType() AggrType   { return aggrTypeCount }

func (a *CountAggregator) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_cnt[%d]=_%s_cnt[%d]+%d;", col, bucket, col, bucket, a.count)
}

func (a *CountAggregator) SetExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_cnt[%d]=%d;", col, bucket, a.count)
}

func (a *CountAggregator) InitExpr(col string, buckets int) string {
	return fmt.Sprintf("_%s_cnt=init_array(%d,'int');", col, buckets)
}

type SumAggregator struct {
	sum float64
}

func (a *SumAggregator) Aggregate(v float64) { a.sum += v }
func (a *SumAggregator) Clear()              { a.sum = 0 }
func (a *SumAggregator) GetType() AggrType   { return aggrTypeSum }

func (a *SumAggregator) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_sum[%d]=_%s_sum[%d]+%f;", col, bucket, col, bucket, a.sum)
}

func (a *SumAggregator) SetExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_sum[%d]=%f;", col, bucket, a.sum)
}

func (a *SumAggregator) InitExpr(col string, buckets int) string {
	return fmt.Sprintf("_%s_sum=init_array(%d,'double');", col, buckets)
}
