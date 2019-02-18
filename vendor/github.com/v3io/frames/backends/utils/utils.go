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

package utils

import (
	"fmt"
	"math"
	"time"

	"github.com/v3io/frames"
)

// AppendValue appends a value to data
func AppendValue(data interface{}, value interface{}) (interface{}, error) {
	switch data.(type) {
	case []int64:
		ival, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("append type mismatch data is %T while value is %T", data, value)
		}
		idata := data.([]int64)
		idata = append(idata, ival)
		return idata, nil
	case []float64:
		fval, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("append type mismatch data is %T while value is %T", data, value)
		}
		fdata := data.([]float64)
		fdata = append(fdata, fval)
		return fdata, nil
	case []string:
		sval, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("append type mismatch data is %T while value is %T", data, value)
		}
		sdata := data.([]string)
		sdata = append(sdata, sval)
		return sdata, nil
	case []time.Time:
		tval, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("append type mismatch data is %T while value is %T", data, value)
		}
		tdata := data.([]time.Time)
		tdata = append(tdata, tval)
		return tdata, nil
	}

	return nil, fmt.Errorf("unsupported data type - %T", data)
}

// NewColumn creates a new column from type of value
func NewColumn(value interface{}, size int) (interface{}, error) {
	switch value.(type) {
	case int64, int32, int16, int8, int:
		return make([]int64, size), nil
	case float64:
		data := make([]float64, size)
		for i := range data {
			data[i] = math.NaN()
		}

		return data, nil
	case string:
		return make([]string, size), nil
	case time.Time:
		return make([]time.Time, size), nil
	case bool:
		return make([]bool, size), nil
	}

	return nil, fmt.Errorf("unknown type - %T", value)
}

// AppendNil appends an empty value to data
func AppendNil(col frames.Column) error {
	var value interface{}
	switch col.DType() {
	case frames.IntType:
		value = int64(0)
	case frames.FloatType:
		value = math.NaN()
	case frames.StringType:
		value = ""
	case frames.TimeType:
		value = time.Unix(0, 0)
	case frames.BoolType:
		value = false
	default:
		return fmt.Errorf("unsupported data type - %d", col.DType())
	}

	return AppendColumn(col, value)
}

// ColAt return value at index i in column as interface{}
// This is a slightly differnt use case than col.ValueAt, also we don't want to
// use defer/recover due to performance overhead
func ColAt(col frames.Column, i int) (interface{}, error) {
	if i < 0 || i >= col.Len() {
		return nil, fmt.Errorf("index %d out of range [0:%d]", i, col.Len()-1)
	}

	switch col.DType() {
	case frames.IntType:
		return col.IntAt(i)
	case frames.FloatType:
		return col.FloatAt(i)
	case frames.StringType:
		return col.StringAt(i)
	case frames.TimeType: // TODO: Does v3io support time.Time?
		asTime, err := col.TimeAt(i)
		if err != nil {
			return nil, err
		}
		return asTime.Format(time.RFC3339Nano), nil // store as time string since v3io doesnt have native time format
		//return col.TimeAt(i), nil
	case frames.BoolType:
		return col.BoolAt(i)
	default:
		return nil, fmt.Errorf("unknown column type - %d", col.DType())
	}
}

// RemoveColumn removes the first column that matches name from columns
// If the column is not found, columns is unchanged
func RemoveColumn(name string, columns []frames.Column) []frames.Column {
	for i, col := range columns {
		if col.Name() == name {
			copy(columns[i:], columns[i+1:])
			columns = columns[:len(columns)-1]
			break
		}
	}

	return columns
}

// TODO: Unite with frameimpl.go
type colAppender interface {
	Append(value interface{}) error
}

// AppendColumn appends a value to a column
func AppendColumn(col frames.Column, value interface{}) error {
	ca, ok := col.(colAppender)
	if !ok {
		return fmt.Errorf("column does not support appending")
	}

	return ca.Append(value)
}
