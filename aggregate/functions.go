package aggregate

import "fmt"

type Aggregator interface {
	Aggregate(v float64)
	Clear()
	GetAttr() string
	UpdateExpr(col string, bucket int) string
	SetExpr(col string, bucket int) string
	InitExpr(col string, buckets int) string
}

type CountAggregator struct {
	count int
}

func (a *CountAggregator) Aggregate(v float64) { a.count++ }
func (a *CountAggregator) Clear()              { a.count = 0 }
func (a *CountAggregator) GetAttr() string     { return "count" }

func (a *CountAggregator) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_count[%d]=_%s_count[%d]+%d;", col, bucket, col, bucket, a.count)
}

func (a *CountAggregator) SetExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_count[%d]=%d;", col, bucket, a.count)
}

func (a *CountAggregator) InitExpr(col string, buckets int) string {
	return fmt.Sprintf("_%s_count=init_array(%d,'int');", col, buckets)
}

type FloatAggregator struct {
	val  float64
	attr string
}

func (a *FloatAggregator) Clear()          { a.val = 0 }
func (a *FloatAggregator) GetAttr() string { return a.attr }
func (a *FloatAggregator) GetVal() float64 { return a.val }
func (a *FloatAggregator) SetExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=%f;", col, a.attr, bucket, a.val)
}

func (a *FloatAggregator) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=_%s_%s[%d]+%f;", col, a.attr, bucket, col, a.attr, bucket, a.val)
}

func (a *FloatAggregator) InitExpr(col string, buckets int) string {
	return fmt.Sprintf("_%s_%s=init_array(%d,'double');", col, a.attr, buckets)
}

// Sum Aggregator
type SumAggregator struct{ FloatAggregator }

func (a *SumAggregator) Aggregate(v float64) { a.val += v }

// Power of 2 Aggregator
type SqrAggregator struct{ FloatAggregator }

func (a *SqrAggregator) Aggregate(v float64) { a.val += v * v }

// Minimum Aggregator
type MinAggregator struct{ FloatAggregator }

func (a *MinAggregator) Aggregate(v float64) {
	if v < a.val {
		a.val = v
	}
}
func (a *MinAggregator) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=min(_%s_%s[%d],%f);", col, a.attr, bucket, col, a.attr, bucket, a.val)
}

// Maximum Aggregator
type MaxAggregator struct{ FloatAggregator }

func (a *MaxAggregator) Aggregate(v float64) {
	if v > a.val {
		a.val = v
	}
}
func (a *MaxAggregator) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=max(_%s_%s[%d],%f);", col, a.attr, bucket, col, a.attr, bucket, a.val)
}
