// +build unit

package pquerier

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseQuery(t *testing.T) {
	testCases := []struct {
		input       string
		output      *SelectParams
		outputTable string
	}{
		{input: "select columnA, columnB",
			output: &SelectParams{RequestedColumns: []RequestedColumn{{Metric: "columnA"}, {Metric: "columnB"}}}},

		{input: "select linear.columnA",
			output: &SelectParams{RequestedColumns: []RequestedColumn{{Metric: "columnA", Interpolator: "linear"}}}},

		{input: "select max(prev.columnA), avg(columnB)",
			output: &SelectParams{RequestedColumns: []RequestedColumn{{Metric: "columnA", Interpolator: "prev", Function: "max"},
				{Metric: "columnB", Function: "avg"}}}},

		{input: "select columnA where columnB = 'tal' and columnC < 'Neiman'",
			output: &SelectParams{RequestedColumns: []RequestedColumn{{Metric: "columnA"}}, Filter: "columnB == 'tal' and columnC < 'Neiman'"}},

		{input: "select max(columnA) group by columnB",
			output: &SelectParams{RequestedColumns: []RequestedColumn{{Metric: "columnA", Function: "max"}}, GroupBy: "columnB"}},

		{input: "select min(columnA) as bambi, max(linear.columnB) as bimba where columnB >= 123 group by columnB,columnC ",
			output: &SelectParams{RequestedColumns: []RequestedColumn{{Metric: "columnA", Function: "min", Alias: "bambi"},
				{Metric: "columnB", Function: "max", Interpolator: "linear", Alias: "bimba"}},
				Filter: "columnB >= 123", GroupBy: "columnB, columnC"}},

		{input: "select min(columnA) from my_table where columnB >= 123",
			output: &SelectParams{RequestedColumns: []RequestedColumn{{Metric: "columnA", Function: "min"}},
				Filter: "columnB >= 123"},
			outputTable: "my_table"},

		{input: "select * from my_table",
			output:      &SelectParams{RequestedColumns: []RequestedColumn{{Metric: ""}}},
			outputTable: "my_table"},

		{input: "select max(*), avg(*) from my_table",
			output:      &SelectParams{RequestedColumns: []RequestedColumn{{Metric: "", Function: "max"}, {Metric: "", Function: "avg"}}},
			outputTable: "my_table"},
	}
	for _, test := range testCases {
		t.Run(test.input, func(tt *testing.T) {
			queryParams, table, err := ParseQuery(test.input)
			if err != nil {
				tt.Fatal(err)
			}

			assert.Equal(tt, test.output, queryParams)
			assert.Equal(tt, test.outputTable, table)
		})
	}
}
