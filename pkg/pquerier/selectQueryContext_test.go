// +build unit

package pquerier

import (
	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"testing"
)

func TestCreateColumnSpecs(t *testing.T) {
	testCases := []struct {
		desc             string
		params           SelectParams
		expectedSpecs    []columnMeta
		expectedSpecsMap map[string][]columnMeta
	}{
		{params: SelectParams{Name: "cpu"},
			expectedSpecs:    []columnMeta{{metric: "cpu"}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu"}}}},

		{params: SelectParams{Name: "cpu", Functions: "count"},
			expectedSpecs:    []columnMeta{{metric: "cpu", function: toAggr("count")}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("count")}}}},

		{params: SelectParams{Name: "cpu", Functions: "avg"},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("avg")},
				{metric: "cpu", function: toAggr("count"), isHidden: true},
				{metric: "cpu", function: toAggr("sum"), isHidden: true}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("avg")},
				{metric: "cpu", function: toAggr("count"), isHidden: true},
				{metric: "cpu", function: toAggr("sum"), isHidden: true}}}},

		{params: SelectParams{Name: "cpu", Functions: "avg,count"},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("avg")},
				{metric: "cpu", function: toAggr("count")},
				{metric: "cpu", function: toAggr("sum"), isHidden: true}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("avg")},
				{metric: "cpu", function: toAggr("count")},
				{metric: "cpu", function: toAggr("sum"), isHidden: true}}}},

		{params: SelectParams{columnSpecs: []RequestedColumn{{metric: "cpu", function: "count"}}},
			expectedSpecs:    []columnMeta{{metric: "cpu", function: toAggr("count")}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("count")}}}},

		{params: SelectParams{columnSpecs: []RequestedColumn{{metric: "cpu", function: "count"},
			{metric: "disk", function: "count"}}},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("count")}, {metric: "disk", function: toAggr("count")}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("count")}},
				"disk": {{metric: "disk", function: toAggr("count")}}}},

		{params: SelectParams{columnSpecs: []RequestedColumn{{metric: "cpu", function: "avg"},
			{metric: "cpu", function: "sum"},
			{metric: "disk", function: "count"}}},
			expectedSpecs: []columnMeta{{metric: "cpu", function: toAggr("avg")},
				{metric: "cpu", function: toAggr("sum")},
				{metric: "cpu", function: toAggr("count"), isHidden: true},
				{metric: "disk", function: toAggr("count")}},
			expectedSpecsMap: map[string][]columnMeta{"cpu": {{metric: "cpu", function: toAggr("avg")},
				{metric: "cpu", function: toAggr("sum")},
				{metric: "cpu", function: toAggr("count"), isHidden: true}},
				"disk": {{metric: "disk", function: toAggr("count")}}}},
	}
	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			ctx := selectQueryContext{}

			columnsSpec, columnsSpecByMetric, err := ctx.createColumnSpecs(&test.params)

			if err != nil {
				t.Fatal(err)
			}
			assert.ElementsMatch(t, test.expectedSpecs, columnsSpec)
			assert.Equal(t, test.expectedSpecsMap, columnsSpecByMetric)
		})
	}
}

func toAggr(str string) aggregate.AggrType {
	_, aggr, _ := aggregate.StrToAggr(str)
	return aggr[0]
}
