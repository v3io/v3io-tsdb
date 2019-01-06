// +build unit

package pquerier

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseQuery(t *testing.T) {
	testCases := []struct {
		input  string
		output *SelectParams
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
	}
	for _, test := range testCases {
		t.Run(test.input, func(tt *testing.T) {
			queryParams, err := ParseQuery(test.input)
			if err != nil {
				tt.Fatal(err)
			}

			assert.Equal(tt, test.output, queryParams)
		})
	}
}
