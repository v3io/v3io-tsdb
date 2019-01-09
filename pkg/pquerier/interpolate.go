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
func GetInterpolateFunc(alg InterpolationType, tolerance int64) InterpolationFunction {
	switch alg {
	case interpolateNaN:
		return func(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
			if !validateTolerance(tolerance, tseek, tprev, tnext) {
				return 0, 0
			}
			return tseek, math.NaN()
		}
	case interpolatePrev:
		return func(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
			if !validateTolerance(tolerance, tseek, tprev, tnext) {
				return 0, 0
			}
			return tseek, vprev
		}
	case interpolateNext:
		return func(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
			if !validateTolerance(tolerance, tseek, tprev, tnext) {
				return 0, 0
			}
			return tseek, vnext
		}
	case interpolateLinear:
		return func(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
			if !validateTolerance(tolerance, tseek, tprev, tnext) {
				return 0, 0
			}
			if math.IsNaN(vprev) || math.IsNaN(vnext) {
				return tseek, math.NaN()
			}
			v := vprev + (vnext-vprev)*float64(tseek-tprev)/float64(tnext-tprev)
			return tseek, v
		}
	default:
		// None interpolation
		return func(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
			if !validateTolerance(tolerance, tseek, tprev, tnext) {
				return 0, 0
			}
			return tnext, vnext
		}
	}
}

func validateTolerance(tolerance, tseek, tprev, tnext int64) bool {
	// If previous point is too far behind for interpolation or the next point is too far ahead then return NaN
	if (tprev != 0 && tseek-tprev > tolerance) || tnext-tseek > tolerance {
		return false
	}
	return true
}
