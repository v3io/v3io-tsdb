package aggregate

import (
	"fmt"
	"testing"
)

func TestAggragators(t *testing.T) {
	aggrString := "*"
	aggr, err := AggrsFromString(aggrString)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("aggr type", aggr)
	aggrList := NewAggregatorList(aggr)
	aggrList.Aggregate(1, 7.5)
	aggrList.Aggregate(2, 3.3)
	fmt.Println(aggrList.UpdateExpr("v", 1))
	fmt.Println(aggrList.SetExpr("v", 1))
}
