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

package pquerier

import (
	"fmt"
	"math"
	"strings"
)

type InterpolationType uint8

const (
	interpolateNone      InterpolationType = 0
	interpolateNaN       InterpolationType = 1
	interpolatePrev      InterpolationType = 2
	interpolateNext      InterpolationType = 3
	interpolateLinear    InterpolationType = 4
	defaultInterpolation InterpolationType = interpolateNext
)

type InterpolationFunction func(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64)

func StrToInterpolateType(str string) (InterpolationType, error) {
	switch strings.ToLower(str) {
	case "none", "":
		return interpolateNone, nil
	case "nan":
		return interpolateNaN, nil
	case "prev":
		return interpolatePrev, nil
	case "next":
		return interpolateNext, nil
	case "lin", "linear":
		return interpolateLinear, nil
	}
	return 0, fmt.Errorf("unknown/unsupported interpulation function %s", str)
}

// return line interpolation function, estimate seek value based on previous and next points
func GetInterpolateFunc(alg InterpolationType) InterpolationFunction {
	switch alg {
	case interpolateNaN:
		return projectNaN
	case interpolatePrev:
		return projectPrev
	case interpolateNext:
		return projectNext
	case interpolateLinear:
		return projectLinear
	default:
		return projectNone
	}
}

// skip to next point
func projectNone(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
	return tnext, vnext
}

// return NaN, there is no valid data in this point
func projectNaN(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
	return tseek, math.NaN()
}

// return same value as in previous point
func projectPrev(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
	return tseek, vprev
}

// return the same value as in the next point
func projectNext(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
	return tseek, vnext
}

// linear estimator (smooth graph)
func projectLinear(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
	if math.IsNaN(vprev) || math.IsNaN(vnext) {
		return tseek, math.NaN()
	}
	v := vprev + (vnext-vprev)*float64(tseek-tprev)/float64(tnext-tprev)
	return tseek, v
}
