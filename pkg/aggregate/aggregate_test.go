// +build unit

package aggregate

import (
	"testing"
	"fmt"
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
	}{
		{"Should aggregate data with Count aggregator", "count", map[int64]float64{1: 7.5, 2: 2.5}, "v", 1,
			"_v_count[1]=_v_count[1]+2;", "_v_count[1]=2;", false},

		{"Should aggregate data with Sum aggregator", "sum", map[int64]float64{1: 7.5, 2: 2.5}, "v", 1,
			fmt.Sprintf("_v_sum[1]=_v_sum[1]+%f;", 10.0),
			fmt.Sprintf("_v_sum[1]=%f;", 10.0), false},

		{"Should aggregate data with Sqr aggregator", "sqr", map[int64]float64{1: 2.0}, "v", 1,
			fmt.Sprintf("_v_sqr[1]=_v_sqr[1]+%f;", 4.0),
			fmt.Sprintf("_v_sqr[1]=%f;", 4.0), false},

			// todo: enable when bug is fixed
		//{"Should aggregate data with Min & Max aggregators", "min,max", map[int64]float64{1: 7.5, 2: 2.5}, "v", 1,
		//	fmt.Sprintf("_v_min[1]=min(_v_min[1],%f);_v_max[1]=max(_v_max[1],%f);", 2.5, 7.5),
		//	fmt.Sprintf("_v_min[1]=%f;_v_max[1]=%f;", 2.5, 7.5), false},

		{"Should aggregate data with Count,Sum,Sqr,Last aggregators", "count,sum,sqr,last", map[int64]float64{1: 7.5, 2: 2.5}, "v", 1,
			fmt.Sprintf("_v_count[1]=_v_count[1]+2;_v_sum[1]=_v_sum[1]+%f;_v_sqr[1]=_v_sqr[1]+%f;_v_last[1]=%f;", 10.0, 62.5, 2.5),
			fmt.Sprintf("_v_count[1]=2;_v_sum[1]=%f;_v_sqr[1]=%f;_v_last[1]=%f;", 10.0, 62.5, 2.5), false},

		// todo: enable when bug is fixed
		//{"Should aggregate data with Wildcard aggregators", "*", map[int64]float64{1: 7.5, 2: 2.5}, "v", 1,
		//	fmt.Sprintf("_v_count[1]=_v_count[1]+2;_v_sum[1]=_v_sum[1]+%f;"+
		//		"_v_sqr[1]=_v_sqr[1]+%f;_v_min[1]=min(_v_min[1],%f);_v_max[1]=max(_v_max[1],%f);"+
		//		"_v_last[1]=%f;", 10.0, 62.5, 2.5, 7.5, 2.5),
		//	fmt.Sprintf("_v_count[1]=2;_v_sum[1]=%f;_v_sqr[1]=%f;"+
		//		"_v_min[1]=%f;_v_max[1]=%f;_v_last[1]=%f;", 10.0, 62.5, 2.5, 7.5, 2.5), false},
		//
		//{"Should aggregate data with Bad aggregator", "not-real", map[int64]float64{1: 7.5, 2: 2.5}, "v", 1,
		//	"_v_count[1]=_v_count[1]+2;", "_v_count[1]=2;", true},
	}

	for _, test := range testCases {
		t.Logf("%s\n", test.desc)
		aggregator, err := AggrsFromString(test.aggString)
		if err != nil {
			if (!test.expectFail){
				t.Fatal(err)
			} else {
				return
			}
		}
		aggregatorList := NewAggregatorList(aggregator)

		for k, v := range test.data {
			aggregatorList.Aggregate(k, v)
		}

		actualUpdateExpr := aggregatorList.UpdateExpr(test.exprCol, test.bucket)
		if actualUpdateExpr != test.expectedUpdateExpr {
			t.Errorf("test: %s failed. actual update Expresion %s is not equal to expected %s",
				test.desc, actualUpdateExpr, test.expectedUpdateExpr)
		}

		actualSetExpr := aggregatorList.SetExpr(test.exprCol, test.bucket)
		if actualSetExpr != test.expectedSetExpr {
			t.Errorf("test: %s failed. actual set Expresion %s is not equal to expected %s",
				test.desc, actualSetExpr, test.expectedSetExpr)
		}
	}
}
