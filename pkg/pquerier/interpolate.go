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

func (it InterpolationType) String() string {
	switch it {
	case interpolateNone:
		return "none"
	case interpolateNaN:
		return "nan"
	case interpolatePrev:
		return "prev"
	case interpolateNext:
		return "next"
	case interpolateLinear:
		return "linear"
	default:
		return "unknown"
	}
}

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
			return tseek, math.NaN()
		}
	case interpolatePrev:
		return func(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
			if absoluteDiff(tseek, tprev) > tolerance {
				return 0, 0
			}
			return tseek, vprev
		}
	case interpolateNext:
		return func(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
			if absoluteDiff(tnext, tseek) > tolerance {
				return 0, 0
			}
			return tseek, vnext
		}
	case interpolateLinear:
		return func(tprev, tnext, tseek int64, vprev, vnext float64) (int64, float64) {
			if (absoluteDiff(tseek, tprev) > tolerance) || absoluteDiff(tnext, tseek) > tolerance {
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
			return tnext, vnext
		}
	}
}

func absoluteDiff(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return b - a
}
