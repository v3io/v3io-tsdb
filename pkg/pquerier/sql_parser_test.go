// +build unit

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

		{input: "select max(prev_val(columnA)), avg(columnB)",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA", Interpolator: "prev_val", Function: "max"},
				{Metric: "columnB", Function: "avg"}}}},

		{input: "select max(next_val(columnA)), avg(columnB)",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA", Interpolator: "next_val", Function: "max"},
				{Metric: "columnB", Function: "avg"}}}},

		{input: "select max(prev_val(columnA, '1h')) as ahsheli, avg(columnB)",
			output: &pquerier.SelectParams{RequestedColumns: []pquerier.RequestedColumn{{Metric: "columnA",
				Interpolator:           "prev_val",
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

func TestNegativeParseQuery(t *testing.T) {
	testCases := []struct {
		input string
	}{
		{input: "select columnA as something, columnB as something"},
		{input: "select avg(columnA) as something, columnB as something"},
		{input: "select avg(*) as something"},
		{input: "select avg(cpu), max(cpu) as cpu"},
	}
	for _, test := range testCases {
		t.Run(test.input, func(tt *testing.T) {
			_, _, err := pquerier.ParseQuery(test.input)
			if err == nil {
				tt.Fatalf("expected error but finished successfully")
			}
		})
	}
}
