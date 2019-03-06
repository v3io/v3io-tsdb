// +build unit

package pquerier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb/tsdbtest"
)

func TestParseQuery(t *testing.T) {
	testCases := []struct {
		input       string
		output      *pquerier.SelectParams
		outputTable string
	}{
		{input: "select columnA, columnB",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA"}, {Metric: "columnB"}}}},

		{input: "select linear(columnA, '10m')",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA",
				Interpolator:           "linear",
				InterpolationTolerance: 10 * tsdbtest.MinuteInMillis}}}},

		{input: "select max(prev(columnA)), avg(columnB)",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA", Interpolator: "prev", Function: "max"},
				{Metric: "columnB", Function: "avg"}}}},

		{input: "select max(prev(columnA, '1h')) as ahsheli, avg(columnB)",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA",
				Interpolator:           "prev",
				Function:               "max",
				Alias:                  "ahsheli",
				InterpolationTolerance: tsdbtest.HoursInMillis},
				{Metric: "columnB", Function: "avg"}}}},

		{input: "select columnA where columnB = 'tal' and columnC < 'Neiman'",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA"}}, Filter: "columnB == 'tal' and columnC < 'Neiman'"}},

		{input: "select max(columnA) group by columnB",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA", Function: "max"}}, GroupBy: "columnB"}},

		{input: "select min(columnA) as bambi, max(linear(columnB)) as bimba where columnB >= 123 group by columnB,columnC ",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA", Function: "min", Alias: "bambi"},
				{Metric: "columnB", Function: "max", Interpolator: "linear", Alias: "bimba"}},
				Filter: "columnB >= 123", GroupBy: "columnB, columnC"}},

		{input: "select min(columnA) from my_table where columnB >= 123",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA", Function: "min"}},
				Filter: "columnB >= 123"},
			outputTable: "my_table"},

		{input: "select * from my_table",
			output:      &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: ""}}},
			outputTable: "my_table"},

		{input: `select * from 'my/table'`,
			output:      &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: ""}}},
			outputTable: "my/table"},

		{input: "select max(*), avg(*) from my_table",
			output:      &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "", Function: "max"}, {Metric: "", Function: "avg"}}},
			outputTable: "my_table"},
	}
	for _, test := range testCases {
		t.Run(test.input, func(tt *testing.T) {
			queryParams, table, err := pquerier.ParseQuery(test.input)
			if err != nil {
				tt.Fatal(err)
			}

			assert.Equal(tt, test.output, queryParams)
			assert.Equal(tt, test.outputTable, table)
		})
	}
}
