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
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math"
)

type Aggregate interface {
	Aggregate(t int64, v float64)
	Clear()
	GetAttr() string
	UpdateExpr(col string, bucket int) string
	SetExpr(col string, bucket int) string
	InitExpr(col string, buckets int) string
}

// Count aggregate
type CountAggregate struct {
	count int
}

func (a *CountAggregate) Aggregate(t int64, v float64) { a.count++ }
func (a *CountAggregate) Clear()                       { a.count = 0 }
func (a *CountAggregate) GetAttr() string              { return "count" }

func (a *CountAggregate) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_count[%d]=_%s_count[%d]+%d;", col, bucket, col, bucket, a.count)
}

func (a *CountAggregate) SetExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_count[%d]=%d;", col, bucket, a.count)
}

func (a *CountAggregate) InitExpr(col string, buckets int) string {
	return fmt.Sprintf("_%s_count=init_array(%d,'int');", col, buckets)
}

// base float64 Aggregate
type FloatAggregate struct {
	val  float64
	attr string
}

func (a *FloatAggregate) Clear()          { a.val = 0 }
func (a *FloatAggregate) GetAttr() string { return a.attr }
func (a *FloatAggregate) GetVal() float64 { return a.val }
func (a *FloatAggregate) SetExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=%s;", col, a.attr, bucket, utils.FloatToNormalizedScientificStr(a.val))
}

func (a *FloatAggregate) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=_%s_%s[%d]+%s;", col, a.attr, bucket, col, a.attr, bucket,
		utils.FloatToNormalizedScientificStr(a.val))
}

func (a *FloatAggregate) InitExpr(col string, buckets int) string {
	return fmt.Sprintf("_%s_%s=init_array(%d,'double',%f);", col, a.attr, buckets, a.val)
}

// Sum Aggregate
type SumAggregate struct{ FloatAggregate }

func (a *SumAggregate) Aggregate(t int64, v float64) {
	if utils.IsDefined(v) {
		a.val += v
	}
}

// Power of 2 Aggregate
type SqrAggregate struct{ FloatAggregate }

func (a *SqrAggregate) Aggregate(t int64, v float64) {
	if utils.IsDefined(v) {
		a.val += v * v
	}
}

// Minimum Aggregate
type MinAggregate struct{ FloatAggregate }

func (a *MinAggregate) Clear() { a.val = math.Inf(1) }

func (a *MinAggregate) Aggregate(t int64, v float64) {
	if v < a.val {
		a.val = v
	}
}
func (a *MinAggregate) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=min(_%s_%s[%d],%s);", col, a.attr, bucket, col, a.attr, bucket,
		utils.FloatToNormalizedScientificStr(a.val))
}

// Maximum Aggregate
type MaxAggregate struct{ FloatAggregate }

func (a *MaxAggregate) Clear() { a.val = math.Inf(-1) }

func (a *MaxAggregate) Aggregate(t int64, v float64) {
	if v > a.val {
		a.val = v
	}
}
func (a *MaxAggregate) UpdateExpr(col string, bucket int) string {
	return fmt.Sprintf("_%s_%s[%d]=max(_%s_%s[%d],%s);", col, a.attr, bucket, col, a.attr, bucket,
		utils.FloatToNormalizedScientificStr(a.val))
}

// Last value Aggregate
type LastAggregate struct {
	FloatAggregate
	lastT int64
}

func (a *LastAggregate) Clear() { a.val = math.Inf(-1) }

func (a *LastAggregate) Aggregate(t int64, v float64) {
	if t > a.lastT {
		a.val = v
		a.lastT = t
	}
}

func (a *LastAggregate) UpdateExpr(col string, bucket int) string {
	if utils.IsUndefined(a.val) {
		return ""
	}

	return fmt.Sprintf("_%s_%s[%d]=%s;", col, a.attr, bucket, utils.FloatToNormalizedScientificStr(a.val))
}
