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
	"math"
)

type Aggregator interface {
	Aggregate(t int64, v float64)
	Clear()
	GetAttr() string
	UpdateExpr(col string, bucket int) string
	SetExpr(col string, bucket int) string
	InitExpr(col string, buckets int) string
}

// Count aggregator
type CountAggregator struct {
	count int
}

func (a *CountAggregator) Aggregate(t int64, v float64) { a.count++ }
func (a *CountAggregator) Clear()                       { a.count = 0 }
func (a *CountAggregator) GetAttr() string              { return "count" }

func (a *CountAggregator) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_count[%d]=_%s_count[%d]+%d;", col, bucket, col, bucket, a.count)
}

func (a *CountAggregator) SetExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_count[%d]=%d;", col, bucket, a.count)
}

func (a *CountAggregator) InitExpr(col string, buckets int) string {
	return fmt.Sprintf("_%s_count=init_array(%d,'int');", col, buckets)
}

// base float64 Aggregator
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

func (a *SumAggregator) Aggregate(t int64, v float64) {
	if !math.IsNaN(v) {
		a.val += v
	}
}

// Power of 2 Aggregator
type SqrAggregator struct{ FloatAggregator }

func (a *SqrAggregator) Aggregate(t int64, v float64) {
	if !math.IsNaN(v) {
		a.val += v * v
	}
}

// Minimum Aggregator
type MinAggregator struct{ FloatAggregator }

func (a *MinAggregator) Clear() { a.val = math.NaN() }

func (a *MinAggregator) Aggregate(t int64, v float64) {
	if !math.IsNaN(v) && (math.IsNaN(a.val) || v < a.val) {
		a.val = v
	}
}
func (a *MinAggregator) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=min(_%s_%s[%d],%f);", col, a.attr, bucket, col, a.attr, bucket, a.val)
}

// Maximum Aggregator
type MaxAggregator struct{ FloatAggregator }

func (a *MaxAggregator) Clear() { a.val = math.NaN() }

func (a *MaxAggregator) Aggregate(t int64, v float64) {
	if !math.IsNaN(v) && (math.IsNaN(a.val) || v > a.val) {
		a.val = v
	}
}
func (a *MaxAggregator) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=max(_%s_%s[%d],%f);", col, a.attr, bucket, col, a.attr, bucket, a.val)
}

// Last value Aggregator
type LastAggregator struct {
	FloatAggregator
	lastT int64
}

func (a *LastAggregator) Clear() { a.val = math.NaN() }

func (a *LastAggregator) Aggregate(t int64, v float64) {
	if t > a.lastT {
		a.val = v
		a.lastT = t
	}
}
func (a *LastAggregator) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=%f;", col, a.attr, bucket, a.val)
}
