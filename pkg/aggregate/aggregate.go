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
	"github.com/v3io/v3io-tsdb/pkg/config"
	"math"
	"strings"
)

type AggrType uint16

// Aggregation functions
const (
	AggregateLabel = "Aggregate"

	aggrTypeCount AggrType = 1
	aggrTypeSum   AggrType = 2
	aggrTypeSqr   AggrType = 4
	aggrTypeMax   AggrType = 8
	aggrTypeMin   AggrType = 16
	aggrTypeLast  AggrType = 32

	// Derived aggregates
	aggrTypeAvg    AggrType = aggrTypeCount | aggrTypeSum
	aggrTypeRate   AggrType = aggrTypeLast | 0x8000
	aggrTypeStddev AggrType = aggrTypeCount | aggrTypeSum | aggrTypeSqr
	aggrTypeStdvar AggrType = aggrTypeCount | aggrTypeSum | aggrTypeSqr | 0x8000
	aggrTypeAll    AggrType = 0xffff
)

var rawAggregates = []AggrType{aggrTypeCount, aggrTypeSum, aggrTypeSqr, aggrTypeMax, aggrTypeMin, aggrTypeLast}

var aggrTypeString = map[string]AggrType{
	"count": aggrTypeCount, "sum": aggrTypeSum, "sqr": aggrTypeSqr, "max": aggrTypeMax, "min": aggrTypeMin,
	"last": aggrTypeLast, "avg": aggrTypeAvg, "rate": aggrTypeRate,
	"stddev": aggrTypeStddev, "stdvar": aggrTypeStdvar, "*": aggrTypeAll}

var aggrToString = map[AggrType]string{
	aggrTypeCount: "count", aggrTypeSum: "sum", aggrTypeSqr: "sqr", aggrTypeMin: "min", aggrTypeMax: "max",
	aggrTypeLast: "last", aggrTypeAvg: "avg", aggrTypeRate: "rate",
	aggrTypeStddev: "stddev", aggrTypeStdvar: "stdvar", aggrTypeAll: "*",
}

var aggrToSchemaField = map[string]config.SchemaField{
	"count":  {Name: "count", Type: "array", Nullable: true, Items: "double"},
	"sum":    {Name: "sum", Type: "array", Nullable: true, Items: "double"},
	"sqr":    {Name: "sqr", Type: "array", Nullable: true, Items: "double"},
	"max":    {Name: "max", Type: "array", Nullable: true, Items: "double"},
	"min":    {Name: "min", Type: "array", Nullable: true, Items: "double"},
	"last":   {Name: "last", Type: "array", Nullable: true, Items: "double"},
	"avg":    {Name: "avg", Type: "array", Nullable: true, Items: "double"},
	"rate":   {Name: "rate", Type: "array", Nullable: true, Items: "double"},
	"stddev": {Name: "stddev", Type: "array", Nullable: true, Items: "double"},
	"stdvar": {Name: "stdvar", Type: "array", Nullable: true, Items: "double"},
}

func (a AggrType) HasAverage() bool {
	return (a & aggrTypeAvg) == aggrTypeAvg
}

func SchemaFieldFromString(aggregates []string, col string) ([]config.SchemaField, error) {
	fieldList := make([]config.SchemaField, 0, len(aggregates))
	for _, s := range aggregates {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			if trimmed == "*" {
				fieldList = make([]config.SchemaField, 0, len(aggrToSchemaField))
				for _, val := range aggrToSchemaField {
					fieldList = append(fieldList, getAggrFullName(val, col))
				}
				return fieldList, nil
			} else {
				field, ok := aggrToSchemaField[trimmed]
				if !ok {
					return nil, fmt.Errorf("invalid aggragator type '%s'", trimmed)
				}
				fieldList = append(fieldList, getAggrFullName(field, col))
			}
		}
	}
	return fieldList, nil
}

func getAggrFullName(field config.SchemaField, col string) config.SchemaField {
	fullName := fmt.Sprintf("_%s_%s", col, field.Name)
	field.Name = fullName
	return field
}

func (a AggrType) String() string { return aggrToString[a] }

func AggregatesToStringList(aggregates string) ([]string, error) {
	aggrs := strings.Split(aggregates, ",")
	aggType, err := AggrsFromString(aggrs)
	if err != nil {
		return nil, err
	}
	var list []string
	for _, aggr := range rawAggregates {
		if aggr&aggType != 0 {
			list = append(list, aggrToString[aggr])
		}
	}

	return list, nil
}

// Convert a comma-separated aggregation-functions string to an aggregates mask
func AggrsFromString(split []string) (AggrType, error) {
	var aggrList AggrType
	var hasAggregates bool
	for _, s := range split {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			aggr, ok := aggrTypeString[trimmed]
			if !ok {
				return aggrList, fmt.Errorf("Invalid aggragate type: '%s'", s)
			}
			hasAggregates = true
			aggrList = aggrList | aggr
		}
	}
	// Always have count aggregate by default
	if hasAggregates {
		aggrList = aggrList | aggrTypeCount
	}
	return aggrList, nil
}

// Create a list of aggregate objects from an aggregates mask
func NewAggregatesList(aggrType AggrType) *AggregatesList {
	list := AggregatesList{}
	if (aggrType & aggrTypeCount) != 0 {
		list = append(list, &CountAggregate{})
	}
	if (aggrType & aggrTypeSum) != 0 {
		list = append(list, &SumAggregate{FloatAggregate{attr: "sum"}})
	}
	if (aggrType & aggrTypeSqr) != 0 {
		list = append(list, &SqrAggregate{FloatAggregate{attr: "sqr"}})
	}
	if (aggrType & aggrTypeMin) != 0 {
		list = append(list, &MinAggregate{FloatAggregate{attr: "min", val: math.Inf(1)}})
	}
	if (aggrType & aggrTypeMax) != 0 {
		list = append(list, &MaxAggregate{FloatAggregate{attr: "max", val: math.Inf(-1)}})
	}
	if (aggrType & aggrTypeLast) != 0 {
		list = append(list, &LastAggregate{FloatAggregate{attr: "last", val: math.Inf(-1)}, 0})
	}
	return &list
}

// List of aggregates
type AggregatesList []Aggregate

// Append a value to all aggregates
func (a AggregatesList) Aggregate(t int64, val interface{}) {
	v := val.(float64)
	for _, aggr := range a {
		aggr.Aggregate(t, v)
	}
}

// Return an update expression for the aggregates in the given aggregates list
func (a AggregatesList) UpdateExpr(col string, bucket int) string {
	expr := ""
	for _, aggr := range a {
		expr = expr + aggr.UpdateExpr(col, bucket)
	}
	return expr
}

// Return an aggregates set expression (first value) or update expression
func (a AggregatesList) SetOrUpdateExpr(col string, bucket int, isNew bool) string {
	if isNew {
		return a.SetExpr(col, bucket)
	}
	return a.UpdateExpr(col, bucket)
}

func (a AggregatesList) SetExpr(col string, bucket int) string {
	expr := ""
	for _, aggr := range a {
		expr = expr + aggr.SetExpr(col, bucket)
	}
	return expr
}

// Return an aggregates array-initialization expression
func (a AggregatesList) InitExpr(col string, buckets int) string {
	expr := ""
	for _, aggr := range a {
		expr = expr + aggr.InitExpr(col, buckets)
	}
	return expr
}

// Clear all aggregates
func (a AggregatesList) Clear() {
	for _, aggr := range a {
		aggr.Clear()
	}
}
