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
	"math"
	"sort"
	"time"

	"github.com/v3io/frames/pb"
)

// ColumnBuilder is interface for building columns
type ColumnBuilder interface {
	Append(value interface{}) error
	At(index int) (interface{}, error)
	Set(index int, value interface{}) error
	Delete(index int) error
	Finish() Column
}

// NewSliceColumnBuilder return a builder for SliceColumn
func NewSliceColumnBuilder(name string, dtype DType, size int) ColumnBuilder {
	msg := &pb.Column{
		Kind:  pb.Column_SLICE,
		Name:  name,
		Dtype: pb.DType(dtype),
	}

	// TODO: pre alloate array. Note that for strings we probably don't want to
	// do this since we'll allocate strings twice - zero value then real value
	return &sliceColumBuilder{
		msg:     msg,
		values:  make(map[int]interface{}),
		deleted: make(map[int]bool),
	}
}

type sliceColumBuilder struct {
	msg     *pb.Column
	values  map[int]interface{}
	deleted map[int]bool
	index   int // next index for append. TODO: Find a better name
}

func (b *sliceColumBuilder) At(index int) (interface{}, error) {
	if index < 0 || index >= b.index {
		return nil, fmt.Errorf("index out of bounds [0:%d]", b.index-1)
	}
	return b.values[index], nil
}

func (b *sliceColumBuilder) Append(value interface{}) error {
	err := b.Set(b.index, value)
	return err
}

func (b *sliceColumBuilder) Set(index int, value interface{}) error {
	var err error
	switch b.msg.Dtype {
	case pb.DType_INTEGER:
		err = b.setInt(index, value)
	case pb.DType_FLOAT:
		err = b.setFloat(index, value)
	case pb.DType_STRING:
		err = b.setString(index, value)
	case pb.DType_TIME:
		err = b.setTime(index, value)
	case pb.DType_BOOLEAN:
		err = b.setBool(index, value)
	default:
		return fmt.Errorf("unknown dtype - %s", b.msg.Dtype)
	}

	if err == nil {
		delete(b.deleted, index) // Undelete
		if index >= b.index {
			b.index = index + 1
		}
	}
	return err
}

func (b *sliceColumBuilder) Delete(index int) error {
	if index < 0 || index >= b.index {
		return fmt.Errorf("index out of bounds: [0:%d]", b.index-1)
	}
	b.deleted[index] = true
	delete(b.values, index)
	return nil
}

func (b *sliceColumBuilder) setInt(index int, value interface{}) error {
	switch value.(type) {
	case int64:
		b.values[index] = value.(int64)
		return nil
	case int:
		b.values[index] = int64(value.(int))
		return nil
	case int8:
		b.values[index] = int64(value.(int8))
		return nil
	case int16:
		b.values[index] = int64(value.(int16))
		return nil
	case int32:
		b.values[index] = int64(value.(int32))
		return nil
	}

	return b.typeError(value)
}

func (b *sliceColumBuilder) setTime(index int, value interface{}) error {
	switch value.(type) {
	case time.Time:
		b.values[index] = value.(time.Time).UnixNano()
		return nil
	case int64:
		b.values[index] = value.(int64)
		return nil
	}

	return b.typeError(value)
}

func (b *sliceColumBuilder) typeError(value interface{}) error {
	return fmt.Errorf("unsupported type for %s slice column - %T", b.msg.Dtype, value)
}

func (b *sliceColumBuilder) setFloat(index int, value interface{}) error {
	switch value.(type) {
	case float64:
		b.values[index] = value.(float64)
		return nil
	case float32:
		b.values[index] = float64(value.(float32))
		return nil
	}

	return b.typeError(value)
}

func (b *sliceColumBuilder) setString(index int, value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return b.typeError(value)
	}

	b.values[index] = s
	return nil
}

func (b *sliceColumBuilder) setBool(index int, value interface{}) error {
	bval, ok := value.(bool)
	if !ok {
		return b.typeError(value)
	}

	b.values[index] = bval
	return nil
}

// TODO: Return error
func (b *sliceColumBuilder) Finish() Column {
	size := b.index - len(b.deleted)

	dels := make([]int, 0, len(b.deleted))
	for i := range b.deleted {
		dels = append(dels, i)
	}
	sort.Ints(dels)

	b.msg.Size = int64(size)
	var set func(i int, value interface{})

	switch b.msg.Dtype {
	case pb.DType_INTEGER:
		b.msg.Ints = make([]int64, size)
		set = func(i int, value interface{}) {
			v, _ := value.(int64)
			b.msg.Ints[i] = v
		}
	case pb.DType_FLOAT:
		b.msg.Floats = make([]float64, size)
		set = func(i int, value interface{}) {
			v, ok := value.(float64)
			if !ok {
				v = math.NaN()
			}
			b.msg.Floats[i] = v
		}
	case pb.DType_STRING:
		b.msg.Strings = make([]string, size)
		set = func(i int, value interface{}) {
			v, _ := value.(string)
			b.msg.Strings[i] = v
		}
	case pb.DType_TIME:
		b.msg.Times = make([]int64, size)
		set = func(i int, value interface{}) {
			v, _ := value.(int64)
			b.msg.Times[i] = v
		}
	case pb.DType_BOOLEAN:
		b.msg.Bools = make([]bool, size)
		set = func(i int, value interface{}) {
			v, _ := value.(bool)
			b.msg.Bools[i] = v
		}
	}

	d := 0
	for i := 0; i < size; i++ {
		for d < len(dels) && i+d >= dels[d] {
			d++
		}
		v := b.values[i+d]
		set(i, v)
	}

	return &colImpl{msg: b.msg}
}

// NewLabelColumnBuilder return a builder for LabelColumn
func NewLabelColumnBuilder(name string, dtype DType, size int) ColumnBuilder {
	msg := &pb.Column{
		Kind:  pb.Column_LABEL,
		Name:  name,
		Dtype: pb.DType(dtype),
		Size:  int64(size),
	}

	switch dtype {
	case IntType:
		msg.Ints = make([]int64, 1)
	case FloatType:
		msg.Floats = make([]float64, 1)
	case StringType:
		msg.Strings = make([]string, 1)
	case TimeType:
		msg.Times = make([]int64, 1)
	case BoolType:
		msg.Bools = make([]bool, 1)
	}

	return &labelColumBuilder{
		msg:     msg,
		empty:   true,
		deleted: make(map[int]bool),
	}
}

type labelColumBuilder struct {
	msg     *pb.Column
	empty   bool
	deleted map[int]bool
}

func (b *labelColumBuilder) At(index int) (interface{}, error) {
	return valueAt(b.msg, index)
}

func (b *labelColumBuilder) Finish() Column {
	b.msg.Size -= int64(len(b.deleted))
	return &colImpl{msg: b.msg}
}

func (b *labelColumBuilder) Append(value interface{}) error {
	return b.Set(int(b.msg.Size), value)
}

func (b *labelColumBuilder) Set(index int, value interface{}) error {
	var err error
	switch b.msg.Dtype {
	case pb.DType_INTEGER:
		err = b.setInt(index, value)
	case pb.DType_FLOAT:
		err = b.setFloat(index, value)
	case pb.DType_STRING:
		err = b.setString(index, value)
	case pb.DType_TIME:
		err = b.setTime(index, value)
	case pb.DType_BOOLEAN:
		err = b.setBool(index, value)
	default:
		return fmt.Errorf("unknown dtype - %s", b.msg.Dtype)
	}

	if err == nil {
		newSize := int64(index + 1)
		if b.msg.Size < newSize {
			b.msg.Size = newSize
		}
	}

	return err
}

func (b *labelColumBuilder) Delete(index int) error {
	b.deleted[index] = true
	return nil
}

func (b *labelColumBuilder) setInt(index int, value interface{}) error {
	var ival int64
	switch value.(type) {
	case int64:
		ival = value.(int64)
	case int:
		ival = int64(value.(int))
	case int8:
		ival = int64(value.(int8))
	case int16:
		ival = int64(value.(int16))
	case int32:
		ival = int64(value.(int32))
	default:
		return b.typeError(value)
	}

	if b.empty {
		b.msg.Ints[0] = ival
		b.empty = false
	} else {
		if b.msg.Ints[0] != ival {
			return b.valueError(b.msg.Ints[0], ival)
		}
	}

	return nil
}

func (b *labelColumBuilder) setFloat(index int, value interface{}) error {
	var fval float64
	switch value.(type) {
	case float64:
		fval = value.(float64)
	case float32:
		fval = float64(value.(float32))
	default:
		return b.typeError(value)
	}

	if b.empty {
		b.msg.Floats[0] = fval
		b.empty = false
	} else {
		if b.msg.Floats[0] != fval {
			return b.valueError(b.msg.Floats[0], fval)
		}
	}

	return nil
}

func (b *labelColumBuilder) setString(index int, value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return b.typeError(value)
	}

	if b.empty {
		b.msg.Strings[0] = s
		b.empty = false
	} else {
		if b.msg.Strings[0] != s {
			return b.valueError(b.msg.Strings[0], s)
		}
	}

	return nil
}

func (b *labelColumBuilder) setBool(index int, value interface{}) error {
	bval, ok := value.(bool)
	if !ok {
		return b.typeError(value)
	}

	if b.empty {
		b.msg.Bools[0] = bval
		b.empty = false
	} else {
		if b.msg.Bools[0] != bval {
			return b.valueError(b.msg.Bools[0], bval)
		}
	}

	return nil
}

func (b *labelColumBuilder) setTime(index int, value interface{}) error {
	var t int64
	switch value.(type) {
	case time.Time:
		t = value.(time.Time).UnixNano()
	case int64:
		t = value.(int64)
	default:
		return b.typeError(value)
	}

	if b.empty {
		b.msg.Times[0] = t
		b.empty = false
	} else {
		if b.msg.Times[0] != t {
			return b.valueError(b.msg.Times[0], t)
		}
	}

	return nil

}

func (b *labelColumBuilder) typeError(value interface{}) error {
	return fmt.Errorf("unsupported type for %s label column - %T", b.msg.Dtype, value)
}

func (b *labelColumBuilder) valueError(current, value interface{}) error {
	return fmt.Errorf("differnt value int %s label column - %v != %v", b.msg.Dtype, value, current)
}

func valueAt(msg *pb.Column, index int) (interface{}, error) {
	if int64(index) >= msg.Size {
		return nil, fmt.Errorf("index out of bounds %d > %d", index, msg.Size-1)
	}

	if msg.Kind == pb.Column_LABEL {
		index = 0
	}

	switch msg.Dtype {
	case pb.DType_INTEGER:
		if len(msg.Ints) < index+1 {
			return nil, nil
		}
		return msg.Ints[index], nil
	case pb.DType_FLOAT:
		if len(msg.Floats) < index+1 {
			return nil, nil
		}
		return msg.Floats[index], nil
	case pb.DType_STRING:
		if len(msg.Strings) < index+1 {
			return nil, nil
		}
		return msg.Strings[index], nil
	case pb.DType_TIME:
		if len(msg.Times) < index+1 {
			return nil, nil
		}
		sec := msg.Times[index] / 1e9
		nsec := msg.Times[index] % 1e9
		return time.Unix(sec, nsec), nil
	case pb.DType_BOOLEAN:
		if len(msg.Bools) < index+1 {
			return nil, nil
		}
		return msg.Bools[index], nil
	}

	return nil, nil
}
