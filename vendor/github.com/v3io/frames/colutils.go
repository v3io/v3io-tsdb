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

package frames

import (
	"fmt"
	"time"
)

// ZeroTime is zero value for time
var ZeroTime time.Time

func checkInBounds(col Column, i int) error {
	if i >= 0 && i < col.Len() {
		return nil
	}

	return fmt.Errorf("index %d out of bounds [0:%d]", i, col.Len())
}

func intAt(col Column, i int) (int64, error) {
	if err := checkInBounds(col, i); err != nil {
		return 0, err
	}

	if col.DType() != IntType {
		return 0, fmt.Errorf("not an int column")
	}

	typedCol, err := col.Ints()
	if err != nil {
		return 0, err
	}

	return typedCol[i], nil
}

func floatAt(col Column, i int) (float64, error) {
	if err := checkInBounds(col, i); err != nil {
		return 0.0, err
	}

	if col.DType() != FloatType {
		return 0, fmt.Errorf("not a float64 column")
	}

	typedCol, err := col.Floats()
	if err != nil {
		return 0.0, err
	}

	return typedCol[i], nil
}

func stringAt(col Column, i int) (string, error) {
	if err := checkInBounds(col, i); err != nil {
		return "", err
	}

	if col.DType() != StringType {
		return "", fmt.Errorf("not a string column")
	}

	typedCol := col.Strings()
	return typedCol[i], nil
}

func timeAt(col Column, i int) (time.Time, error) {
	if err := checkInBounds(col, i); err != nil {
		return ZeroTime, err
	}

	if col.DType() != TimeType {
		return ZeroTime, fmt.Errorf("not a time.Time column")
	}

	typedCol, err := col.Times()
	if err != nil {
		return ZeroTime, err
	}

	return typedCol[i], nil
}

func boolAt(col Column, i int) (bool, error) {
	if err := checkInBounds(col, i); err != nil {
		return false, err
	}

	if col.DType() != BoolType {
		return false, fmt.Errorf("not a bool column")
	}

	typedCol, err := col.Bools()
	if err != nil {
		return false, err
	}

	return typedCol[i], nil
}
