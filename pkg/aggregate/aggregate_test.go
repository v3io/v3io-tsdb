// +build unit

package aggregate

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestAggregators(t *testing.T) {
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
		{desc: "Should aggregate data with Count aggregator",
			aggString: "count",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: "_v_count[1]=_v_count[1]+2;", expectedSetExpr: "_v_count[1]=2;"},

		{desc: "Should aggregate data with Sum aggregator",
			aggString: "sum",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_sum[1]=_v_sum[1]+%f;", 10.0),
			expectedSetExpr:    fmt.Sprintf("_v_sum[1]=%f;", 10.0)},

		{desc: "Should aggregate data with Sqr aggregator",
			aggString: "sqr",
			data:      map[int64]float64{1: 2.0},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_sqr[1]=_v_sqr[1]+%f;", 4.0),
			expectedSetExpr:    fmt.Sprintf("_v_sqr[1]=%f;", 4.0)},

		{desc: "Should aggregate data with Min & Max aggregators",
			aggString: "min,max",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_min[1]=min(_v_min[1],%f);_v_max[1]=max(_v_max[1],%f);", 2.5, 7.5),
			expectedSetExpr:    fmt.Sprintf("_v_min[1]=%f;_v_max[1]=%f;", 2.5, 7.5),
			ignoreReason:       "enable when bug is fixed - IG-8675"},

		{desc: "Should aggregate data with Count,Sum,Sqr,Last aggregators",
			aggString: "count,sum,sqr,last",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_count[1]=_v_count[1]+2;_v_sum[1]=_v_sum[1]+%f;_v_sqr[1]=_v_sqr[1]+%f;_v_last[1]=%f;", 10.0, 62.5, 2.5),
			expectedSetExpr:    fmt.Sprintf("_v_count[1]=2;_v_sum[1]=%f;_v_sqr[1]=%f;_v_last[1]=%f;", 10.0, 62.5, 2.5)},

		{desc: "Should aggregate data with Wildcard aggregators",
			aggString: "*",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: fmt.Sprintf("_v_count[1]=_v_count[1]+2;_v_sum[1]=_v_sum[1]+%f;"+
				"_v_sqr[1]=_v_sqr[1]+%f;_v_min[1]=min(_v_min[1],%f);_v_max[1]=max(_v_max[1],%f);"+
				"_v_last[1]=%f;", 10.0, 62.5, 2.5, 7.5, 2.5),
			expectedSetExpr: fmt.Sprintf("_v_count[1]=2;_v_sum[1]=%f;_v_sqr[1]=%f;"+
				"_v_min[1]=%f;_v_max[1]=%f;_v_last[1]=%f;", 10.0, 62.5, 2.5, 7.5, 2.5),
			ignoreReason: "enable when bug is fixed - IG-8675"},

		{desc: "Should aggregate data with Bad aggregator",
			aggString: "not-real",
			data:      map[int64]float64{1: 7.5, 2: 2.5},
			exprCol:   "v", bucket: 1,
			expectedUpdateExpr: "_v_count[1]=_v_count[1]+2;", expectedSetExpr: "_v_count[1]=2;", expectFail: true},
	}

	for _, test := range testCases {
		t.Logf("%s\n", test.desc)
		t.Run(test.desc, func(t *testing.T) {
			if test.ignoreReason != "" {
				t.Skip(test.ignoreReason)
			}
			testAggregatorCase(t, test.aggString, test.data, test.exprCol, test.bucket, test.expectedUpdateExpr,
				test.expectedSetExpr, test.expectFail)
		})
	}
}

func testAggregatorCase(t *testing.T, aggString string, data map[int64]float64, exprCol string, bucket int,
	expectedUpdateExpr string, expectedSetExpr string, expectFail bool) {

	aggregator, err := AggrsFromString(strings.Split(aggString, ","))
	if err != nil {
		if !expectFail {
			t.Fatal(err)
		} else {
			return
		}
	}
	aggregatorList := NewAggregatorList(aggregator)

	for k, v := range data {
		aggregatorList.Aggregate(k, v)
	}

	actualUpdateExpr := strings.Split(aggregatorList.UpdateExpr(exprCol, bucket), ";")
	expectedUpdateExprSet := strings.Split(expectedUpdateExpr, ";")
	assert.ElementsMatch(t, actualUpdateExpr, expectedUpdateExprSet)

	actualSetExpr := strings.Split(aggregatorList.SetExpr(exprCol, bucket), ";")
	expectedSetExprSet := strings.Split(expectedSetExpr, ";")
	assert.ElementsMatch(t, actualSetExpr, expectedSetExprSet)
}
