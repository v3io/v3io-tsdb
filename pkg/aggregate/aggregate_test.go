// +build unit

package aggregate

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
	"testing"
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

	aggregates, err := AggrsFromString(strings.Split(aggString, ","))
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
