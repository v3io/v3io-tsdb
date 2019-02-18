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
	"time"

	"github.com/pkg/errors"

	"github.com/v3io/frames/pb"
)

// frameImpl is a frame implementation
type frameImpl struct {
	labels  map[string]interface{}
	byName  map[string]Column // name -> Column
	columns []Column
	indices []Column
	msg     *pb.Frame
	names   []string // Created on 1st call to Names
}

// NewFrame returns a new Frame
func NewFrame(columns []Column, indices []Column, labels map[string]interface{}) (Frame, error) {
	if err := checkEqualLen(columns, indices); err != nil {
		return nil, err
	}

	msg := &pb.Frame{}

	var err error
	msg.Columns, err = cols2PB(columns)
	if err != nil {
		return nil, err
	}

	msg.Indices, err = cols2PB(indices)
	if err != nil {
		return nil, err
	}

	msg.Labels, err = pb.FromGoMap(labels)
	if err != nil {
		return nil, err
	}

	byName := make(map[string]Column)
	for _, col := range columns {
		byName[col.Name()] = col
	}

	frame := &frameImpl{
		byName:  byName,
		msg:     msg,
		labels:  labels,
		indices: indices,
		columns: columns,
	}

	return frame, nil
}

// NewFrameFromMap returns a new MapFrame from a map
func NewFrameFromMap(columns map[string]interface{}, indices map[string]interface{}) (Frame, error) {
	cols, err := mapToColumns(columns)
	if err != nil {
		return nil, err
	}

	idx, err := mapToColumns(indices)
	if err != nil {
		return nil, err
	}

	return NewFrame(cols, idx, nil)
}

// NewFrameFromRows creates a new frame from rows
func NewFrameFromRows(rows []map[string]interface{}, indices []string, labels map[string]interface{}) (Frame, error) {
	frameCols := make(map[string]Column)
	for rowNum, row := range rows {
		for name, value := range row {
			col, ok := frameCols[name]

			if !ok {
				var err error
				col, err = newColumn(name, value)
				if err != nil {
					return nil, err
				}
				frameCols[name] = col
			}

			extendCol(col, rowNum)
			if err := colAppend(col, value); err != nil {
				return nil, err
			}
		}

		// Extend columns not in row
		for name, col := range frameCols {
			if _, ok := row[name]; !ok {
				extendCol(col, rowNum+1)
			}
		}
	}

	var dataCols, indexCols []Column
	for name, col := range frameCols {
		if inSlice(name, indices) {
			indexCols = append(indexCols, col)
		} else {
			dataCols = append(dataCols, col)
		}
	}

	return NewFrame(dataCols, indexCols, labels)
}

// Names returns the column names
func (fr *frameImpl) Names() []string {
	if fr.names == nil {
		names := make([]string, len(fr.msg.Columns))
		for i, col := range fr.columns {
			names[i] = col.Name()
		}
		fr.names = names
	}
	return fr.names
}

// Indices returns the index columns
func (fr *frameImpl) Indices() []Column {
	return fr.indices
}

// Labels returns the Label set, nil if there's none
func (fr *frameImpl) Labels() map[string]interface{} {
	if fr.labels == nil {
		fr.labels = pb.AsGoMap(fr.msg.Labels)
	}
	return fr.labels
}

// Len is the number of rows
func (fr *frameImpl) Len() int {
	if len(fr.columns) > 0 {
		return fr.columns[0].Len()
	}

	return 0
}

// Column gets a column by name
func (fr *frameImpl) Column(name string) (Column, error) {
	// TODO: We can speed it up by calculating once, but then we'll use more memory
	col, ok := fr.byName[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found", name)
	}

	return col, nil
}

// Slice return a new Frame with is slice of the original
func (fr *frameImpl) Slice(start int, end int) (Frame, error) {
	if err := validateSlice(start, end, fr.Len()); err != nil {
		return nil, err
	}

	colSlices, err := sliceCols(fr.columns, start, end)
	if err != nil {
		return nil, err
	}

	indexSlices, err := sliceCols(fr.indices, start, end)
	if err != nil {
		return nil, err
	}

	return NewFrame(colSlices, indexSlices, fr.labels)
}

// FrameRowIterator returns iterator over rows
func (fr *frameImpl) IterRows(includeIndex bool) RowIterator {
	return newRowIterator(fr, includeIndex)
}

// Proto returns the underlying protobuf message
func (fr *frameImpl) Proto() *pb.Frame {
	return fr.msg
}

// NewFrameFromProto return a new frame from protobuf message
func NewFrameFromProto(msg *pb.Frame) Frame {
	byName := make(map[string]Column)
	columns := make([]Column, len(msg.Columns))
	for i, colMsg := range msg.Columns {
		col := &colImpl{msg: colMsg}
		columns[i] = col
		byName[colMsg.Name] = col
	}
	indices := make([]Column, len(msg.Indices))
	for i, colMsg := range msg.Indices {
		indices[i] = &colImpl{msg: colMsg}
	}

	return &frameImpl{
		msg:     msg,
		columns: columns,
		indices: indices,
		byName:  byName,
	}
}

func validateSlice(start int, end int, size int) error {
	if start < 0 || end < 0 {
		return fmt.Errorf("negative indexing not supported")
	}

	if end < start {
		return fmt.Errorf("end < start")
	}

	if start >= size {
		return fmt.Errorf("start out of bounds")
	}

	if end >= size {
		return fmt.Errorf("end out of bounds")
	}

	return nil
}

func checkEqualLen(columns []Column, indices []Column) error {
	size := -1
	for _, col := range columns {
		if size == -1 { // first column
			size = col.Len()
			continue
		}

		if colSize := col.Len(); colSize != size {
			return fmt.Errorf("%q column size mismatch (%d != %d)", col.Name(), colSize, size)
		}
	}

	for i, col := range indices {
		if colSize := col.Len(); colSize != size {
			return fmt.Errorf("index column %d size mismatch (%d != %d)", i, colSize, size)
		}
	}

	return nil
}

func sliceCols(columns []Column, start int, end int) ([]Column, error) {
	slices := make([]Column, len(columns))
	for i, col := range columns {
		slice, err := col.Slice(start, end)
		if err != nil {
			return nil, errors.Wrapf(err, "can't get slice from %q", col.Name())
		}

		slices[i] = slice
	}

	return slices, nil
}

func zeroValue(dtype DType) (interface{}, error) {
	switch dtype {
	case IntType:
		return int64(0), nil
	case FloatType:
		return math.NaN(), nil
	case StringType:
		return "", nil
	case TimeType:
		return time.Unix(0, 0), nil
	case BoolType:
		return false, nil
	}

	return nil, fmt.Errorf("unsupported data type - %d", dtype)
}

// TODO: Unite with backend/utils.AppendNil
func extendCol(col Column, size int) error {
	if col.Len() >= size {
		return nil
	}

	value, err := zeroValue(col.DType())
	if err != nil {
		return err
	}

	for col.Len() < size {
		if err := colAppend(col, value); err != nil {
			return err
		}
	}

	return nil
}

// TODO: Unite with backend/utils
func newColumn(name string, value interface{}) (Column, error) {
	var data interface{}
	switch value.(type) {
	case int64, int32, int16, int8, int:
		data = []int64{}
	case float64, float32:
		data = []float64{}
	case string:
		data = []string{}
	case time.Time:
		data = []time.Time{}
	case bool:
		data = []bool{}
	default:
		return nil, fmt.Errorf("unsupported type %T", value)
	}

	return NewSliceColumn(name, data)
}

func inSlice(name string, names []string) bool {
	for _, n := range names {
		if name == n {
			return true
		}
	}

	return false
}

func mapToColumns(data map[string]interface{}) ([]Column, error) {
	columns := make([]Column, len(data))
	i := 0

	for name, values := range data {
		column, err := NewSliceColumn(name, values)
		if err != nil {
			return nil, errors.Wrapf(err, "can't create column %q", name)
		}
		columns[i] = column
		i++
	}

	return columns, nil
}

type colAppender interface {
	Append(value interface{}) error
}

func colAppend(col Column, value interface{}) error {
	ca, ok := col.(colAppender)
	if !ok {
		return fmt.Errorf("column does not support appending")
	}

	return ca.Append(value)
}

func cols2PB(columns []Column) ([]*pb.Column, error) {
	pbCols := make([]*pb.Column, len(columns))
	for i, col := range columns {
		pbCol, ok := col.(*colImpl)
		if !ok {
			return nil, fmt.Errorf("%d: column %q is not protobuf", i, col.Name())
		}
		pbCols[i] = pbCol.msg
	}

	return pbCols, nil
}
