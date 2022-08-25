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

package aggregate

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

func TestAggregates(t *testing.T) {
	testCases := []struct {
		desc               string
		aggString          string
		data               map[int64]float64
		exprCol            string
		bucket             int
		expectedUpdateExpr string
		expectedSetExpr    string
		expectFail         bool
		ignoreReason       string
	}{
		{desc: "Should aggregate data with Count aggregate",
			aggString: "count",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: "_v_count[1]=_v_count[1]+2;", expectedSetExpr: "_v_count[1]=2;"},

		{desc: "Should aggregate data with Sum aggregate",
			aggString: "sum",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_sum[1]=_v_sum[1]+%s;_v_count[1]=_v_count[1]+2;", utils.FloatToNormalizedScientificStr(10.0)),
			expectedSetExpr:    fmt.Sprintf("_v_sum[1]=%s;_v_count[1]=2;", utils.FloatToNormalizedScientificStr(10.0))},

		{desc: "Should aggregate data with Sqr aggregate",
			aggString: "sqr",
			data:      map[int64]float64{1: 2.0},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_sqr[1]=_v_sqr[1]+%s;_v_count[1]=_v_count[1]+1;", utils.FloatToNormalizedScientificStr(4.0)),
			expectedSetExpr:    fmt.Sprintf("_v_sqr[1]=%s;_v_count[1]=1;", utils.FloatToNormalizedScientificStr(4.0))},

		{desc: "Should aggregate data with Min & Max aggregates",
			aggString: "min,max",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_min[1]=min(_v_min[1],%s);_v_max[1]=max(_v_max[1],%s);_v_count[1]=_v_count[1]+2;",
				utils.FloatToNormalizedScientificStr(2.5), utils.FloatToNormalizedScientificStr(7.5)),
			expectedSetExpr: fmt.Sprintf("_v_min[1]=%s;_v_max[1]=%s;_v_count[1]=2;",
				utils.FloatToNormalizedScientificStr(2.5),
				utils.FloatToNormalizedScientificStr(7.5))},

		{desc: "Should aggregate data with Count,Sum,Sqr,Last aggregates",
			aggString: "count,sum,sqr,last",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_count[1]=_v_count[1]+2;_v_sum[1]=_v_sum[1]+%s;_v_sqr[1]=_v_sqr[1]+%s;_v_last[1]=%s;",
				utils.FloatToNormalizedScientificStr(10.0), utils.FloatToNormalizedScientificStr(62.5),
				utils.FloatToNormalizedScientificStr(2.5)),
			expectedSetExpr: fmt.Sprintf("_v_count[1]=2;_v_sum[1]=%s;_v_sqr[1]=%s;_v_last[1]=%s;",
				utils.FloatToNormalizedScientificStr(10.0),
				utils.FloatToNormalizedScientificStr(62.5), utils.FloatToNormalizedScientificStr(2.5))},

		{desc: "Should aggregate data with Wildcard aggregates",
			aggString: "*",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_count[1]=_v_count[1]+2;_v_sum[1]=_v_sum[1]+%s;"+
				"_v_sqr[1]=_v_sqr[1]+%s;_v_min[1]=min(_v_min[1],%s);_v_max[1]=max(_v_max[1],%s);"+
				"_v_last[1]=%s;", utils.FloatToNormalizedScientificStr(10.0),
				utils.FloatToNormalizedScientificStr(62.5),
				utils.FloatToNormalizedScientificStr(2.5), utils.FloatToNormalizedScientificStr(7.5),
				utils.FloatToNormalizedScientificStr(2.5)),
			expectedSetExpr: fmt.Sprintf("_v_count[1]=2;_v_sum[1]=%s;_v_sqr[1]=%s;"+
				"_v_min[1]=%s;_v_max[1]=%s;_v_last[1]=%s;",
				utils.FloatToNormalizedScientificStr(10.0), utils.FloatToNormalizedScientificStr(62.5),
				utils.FloatToNormalizedScientificStr(2.5), utils.FloatToNormalizedScientificStr(7.5),
				utils.FloatToNormalizedScientificStr(2.5))},

		{desc: "Should aggregate data with Bad aggregate",
			aggString: "not-real",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: "_v_count[1]=_v_count[1]+2;", expectedSetExpr: "_v_count[1]=2;", expectFail: true},

		{desc: "Should aggregate data when specifying aggregates with sapces",
			aggString: "min , max   ",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_min[1]=min(_v_min[1],%s);_v_max[1]=max(_v_max[1],%s);_v_count[1]=_v_count[1]+2;",
				utils.FloatToNormalizedScientificStr(2.5), utils.FloatToNormalizedScientificStr(7.5)),
			expectedSetExpr: fmt.Sprintf("_v_min[1]=%s;_v_max[1]=%s;_v_count[1]=2;",
				utils.FloatToNormalizedScientificStr(2.5), utils.FloatToNormalizedScientificStr(7.5))},

		{desc: "Should aggregate data when specifying aggregates with empty values",
			aggString: "min , ,max   ",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_min[1]=min(_v_min[1],%s);_v_max[1]=max(_v_max[1],%s);_v_count[1]=_v_count[1]+2;",
				utils.FloatToNormalizedScientificStr(2.5), utils.FloatToNormalizedScientificStr(7.5)),
			expectedSetExpr: fmt.Sprintf("_v_min[1]=%s;_v_max[1]=%s;_v_count[1]=2;",
				utils.FloatToNormalizedScientificStr(2.5), utils.FloatToNormalizedScientificStr(7.5))},
	}

	for _, test := range testCases {
		t.Logf("%s\n", test.desc)
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testAggregateCase(t, test.aggString, test.data, test.exprCol, test.bucket, test.expectedUpdateExpr,
				test.expectedSetExpr, test.expectFail)
		})
	}
}

func testAggregateCase(t *testing.T, aggString string, data map[int64]float64, exprCol string, bucket int,
	expectedUpdateExpr string, expectedSetExpr string, expectFail bool) {

	aggregates, _, err := AggregatesFromStringListWithCount(strings.Split(aggString, ","))
	if err != nil {
		if !expectFail {
			t.Fatal(err)
		} else {
			return
		}
	}
	aggregatesList := NewAggregatesList(aggregates)

	for k, v := range data {
		aggregatesList.Aggregate(k, v)
	}

	actualUpdateExpr := strings.Split(aggregatesList.UpdateExpr(exprCol, bucket), ";")
	expectedUpdateExprSet := strings.Split(expectedUpdateExpr, ";")
	assert.ElementsMatch(t, actualUpdateExpr, expectedUpdateExprSet)

	actualSetExpr := strings.Split(aggregatesList.SetExpr(exprCol, bucket), ";")
	expectedSetExprSet := strings.Split(expectedSetExpr, ";")
	assert.ElementsMatch(t, actualSetExpr, expectedSetExprSet)
}
