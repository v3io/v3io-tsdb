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
// +build unit

package pquerier

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
)

func TestCreateColumnSpecs(t *testing.T) {
	testCases := []struct {
		desc             string
		params           SelectParams
		expectedSpecs    []columnMeta
		expectedSpecsMap map[string][]columnMeta
	}{
		{params: SelectParams{Name: "cpu"},
			expectedSpecs:    []columnMeta{{metric: "cpu", interpolationType: interpolateNext}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", interpolationType: interpolateNext}}}},

		{params: SelectParams{Name: "cpu", Functions: "count"},
			expectedSpecs:    []columnMeta{{metric: "cpu", function: toAggr("count"), interpolationType: interpolateNext}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("count"), interpolationType: interpolateNext}}}},

		{params: SelectParams{Name: "cpu", Functions: "avg"},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("avg"), interpolationType: interpolateNext},
				{metric: "cpu", function: toAggr("count"), isHidden: true, interpolationType: interpolateNext},
				{metric: "cpu", function: toAggr("sum"), isHidden: true, interpolationType: interpolateNext}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("avg"), interpolationType: interpolateNext},
				{metric: "cpu", function: toAggr("count"), isHidden: true, interpolationType: interpolateNext},
				{metric: "cpu", function: toAggr("sum"), isHidden: true, interpolationType: interpolateNext}}}},

		{params: SelectParams{Name: "cpu", Functions: "avg,count"},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("avg"), interpolationType: interpolateNext},
				{metric: "cpu", function: toAggr("count"), interpolationType: interpolateNext},
				{metric: "cpu", function: toAggr("sum"), isHidden: true, interpolationType: interpolateNext}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("avg"), interpolationType: interpolateNext},
				{metric: "cpu", function: toAggr("count"), interpolationType: interpolateNext},
				{metric: "cpu", function: toAggr("sum"), isHidden: true, interpolationType: interpolateNext}}}},

		{params: SelectParams{RequestedColumns: []RequestedColumn{{Metric: "cpu", Function: "count"}}},
			expectedSpecs:    []columnMeta{{metric: "cpu", function: toAggr("count")}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("count")}}}},

		{params: SelectParams{RequestedColumns: []RequestedColumn{{Metric: "cpu", Function: "count"},
			{Metric: "disk", Function: "count"}}},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("count")}, {metric: "disk", function: toAggr("count")}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("count")}},
				"disk": {{metric: "disk", function: toAggr("count")}}}},

		{params: SelectParams{RequestedColumns: []RequestedColumn{{Metric: "cpu", Function: "avg"},
			{Metric: "cpu", Function: "sum"},
			{Metric: "disk", Function: "count"}}},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("avg")},
				{metric: "cpu", function: toAggr("sum")},
				{metric: "cpu", function: toAggr("count"), isHidden: true},
				{metric: "disk", function: toAggr("count")}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("avg")},
				{metric: "cpu", function: toAggr("sum")},
				{metric: "cpu", function: toAggr("count"), isHidden: true}},
				"disk": {{metric: "disk", function: toAggr("count")}}}},

		{params: SelectParams{Name: "cpu,diskio"},
			expectedSpecs: []columnMeta{{metric: "cpu", interpolationType: interpolateNext},
				{metric: "diskio", interpolationType: interpolateNext}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", interpolationType: interpolateNext}},
				"diskio": {{metric: "diskio", interpolationType: interpolateNext}}}},

		{params: SelectParams{Name: "cpu, diskio", Functions: "sum,count"},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("count"), interpolationType: interpolateNext},
				{metric: "cpu", function: toAggr("sum"), interpolationType: interpolateNext},
				{metric: "diskio", function: toAggr("count"), interpolationType: interpolateNext},
				{metric: "diskio", function: toAggr("sum"), interpolationType: interpolateNext}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("sum"), interpolationType: interpolateNext},
				{metric: "cpu", function: toAggr("count"), interpolationType: interpolateNext}},
				"diskio": {{metric: "diskio", function: toAggr("sum"), interpolationType: interpolateNext},
					{metric: "diskio", function: toAggr("count"), interpolationType: interpolateNext}}}},

		{params: SelectParams{RequestedColumns: []RequestedColumn{{Metric: "cpu", Function: "sum", Interpolator: "linear"},
			{Metric: "cpu", Function: "count", Interpolator: "linear"}}},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("sum"), interpolationType: interpolateLinear},
				{metric: "cpu", function: toAggr("count"), interpolationType: interpolateLinear}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("sum"), interpolationType: interpolateLinear},
				{metric: "cpu", function: toAggr("count"), interpolationType: interpolateLinear}}}},

		{params: SelectParams{RequestedColumns: []RequestedColumn{{Metric: "cpu", Function: "sum", Interpolator: "linear"},
			{Metric: "cpu", Function: "count"}}},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("sum"), interpolationType: interpolateLinear},
				{metric: "cpu", function: toAggr("count"), interpolationType: interpolateLinear}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("sum"), interpolationType: interpolateLinear},
				{metric: "cpu", function: toAggr("count"), interpolationType: interpolateLinear}}}},

		{params: SelectParams{RequestedColumns: []RequestedColumn{{Metric: "cpu", Function: "avg", Interpolator: "linear"},
			{Metric: "cpu", Function: "count"}}},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("avg"), interpolationType: interpolateLinear},
				{metric: "cpu", function: toAggr("sum"), interpolationType: interpolateLinear, isHidden: true},
				{metric: "cpu", function: toAggr("count"), interpolationType: interpolateLinear}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("avg"), interpolationType: interpolateLinear},
				{metric: "cpu", function: toAggr("count"), interpolationType: interpolateLinear},
				{metric: "cpu", function: toAggr("sum"), interpolationType: interpolateLinear, isHidden: true}}}},

		{params: SelectParams{RequestedColumns: []RequestedColumn{{Metric: "cpu", Function: "count", Interpolator: "linear"},
			{Metric: "diskio", Function: "count", Interpolator: "prev_val"},
			{Metric: "diskio", Function: "sum"}}},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("count"), interpolationType: interpolateLinear},
				{metric: "diskio", function: toAggr("count"), interpolationType: interpolatePrev},
				{metric: "diskio", function: toAggr("sum"), interpolationType: interpolatePrev}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("count"), interpolationType: interpolateLinear}},
				"diskio": {
					{metric: "diskio", function: toAggr("count"), interpolationType: interpolatePrev},
					{metric: "diskio", function: toAggr("sum"), interpolationType: interpolatePrev}}}},
	}
	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			ctx := selectQueryContext{}
			ctx.queryParams = &test.params
			columnsSpec, columnsSpecByMetric, err := ctx.createColumnSpecs()

			if err != nil {
				t.Fatal(err)
			}
			assert.ElementsMatch(t, test.expectedSpecs, columnsSpec)
			assert.Equal(t, test.expectedSpecsMap, columnsSpecByMetric)
		})
	}
}

func TestNegativeCreateColumnSpecs(t *testing.T) {
	testCases := []struct {
		desc   string
		params SelectParams
	}{
		{params: SelectParams{Name: "cpu", Functions: "count, count"}},

		{params: SelectParams{Name: "cpu", Functions: "count, max,count"}},

		{params: SelectParams{RequestedColumns: []RequestedColumn{{Metric: "cpu", Function: "count"},
			{Metric: "cpu", Function: "count"}}}},

		{params: SelectParams{RequestedColumns: []RequestedColumn{{Metric: "cpu", Function: "count"},
			{Metric: "diskio", Function: "count"},
			{Metric: "cpu", Function: "count"}}}},

		{params: SelectParams{RequestedColumns: []RequestedColumn{{Metric: "cpu", Function: "count"},
			{Metric: "diskio", Function: "count"},
			{Metric: "cpu", Function: "  count "}}}},

		{params: SelectParams{Name: "cpu", Functions: "count, count", UseOnlyClientAggr: true, disableClientAggr: true}},
	}
	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			ctx := selectQueryContext{}
			ctx.queryParams = &test.params
			_, _, err := ctx.createColumnSpecs()

			if err == nil {
				t.Fatal("expected error but finished normally")
			}
		})
	}
}

func toAggr(str string) aggregate.AggrType {
	aggr, _ := aggregate.FromString(str)
	return aggr
}
