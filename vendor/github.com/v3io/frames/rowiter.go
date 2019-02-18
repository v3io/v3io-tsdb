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
	"sync"
)

type rowIterator struct {
	// names for columns with no name
	noNames      map[int]string
	columns      []Column
	err          error
	frame        Frame
	includeIndex bool
	numCols      int
	once         sync.Once
	row          map[string]interface{}
	rowNum       int
}

func newRowIterator(frame Frame, includeIndex bool) *rowIterator {
	return &rowIterator{
		frame:        frame,
		includeIndex: includeIndex,
	}
}

func (it *rowIterator) init() {
	names := it.frame.Names()
	it.numCols = len(names)
	size := len(names)
	if it.includeIndex {
		size += len(it.frame.Indices())
	}

	nameMap := make(map[string]bool)
	columns := make([]Column, size)
	noNames := make(map[int]string)
	for i, name := range names {
		// FIXME: Currently we allow duplicate names
		col, err := it.frame.Column(name)
		if err != nil {
			it.err = err
			return
		}

		if name == "" {
			name = fmt.Sprintf("col-%d", i)
			noNames[i] = name
		}

		if _, ok := nameMap[name]; ok {
			it.err = fmt.Errorf("duplicate column name - %q", name)
			return
		}

		nameMap[name] = true
		columns[i] = col
	}

	if it.includeIndex {
		for i, col := range it.frame.Indices() {
			name := col.Name()
			if name == "" {
				name = fmt.Sprintf("index-%d", i)
				noNames[it.numCols+i] = name
			}

			if _, ok := nameMap[name]; ok {
				it.err = fmt.Errorf("duplicate name in columns and indices - %q", name)
				return
			}

			nameMap[name] = true
			columns[it.numCols+i] = col
		}
	}

	it.noNames = noNames
	it.columns = columns
}

func (it *rowIterator) Next() bool {
	it.once.Do(it.init)

	if it.err != nil || it.rowNum >= it.frame.Len() {
		return false
	}

	it.row, it.err = it.getRow()
	if it.err != nil {
		return false
	}

	it.rowNum++
	return true
}

func (it *rowIterator) Err() error {
	return it.err
}

func (it *rowIterator) Row() map[string]interface{} {
	if it.err != nil {
		return nil
	}

	return it.row
}

func (it *rowIterator) Indices() map[string]interface{} {
	return nil // TODO
}

func (it *rowIterator) RowNum() int {
	return it.rowNum - 1
}

func (it *rowIterator) getRow() (map[string]interface{}, error) {
	row := make(map[string]interface{})
	for colNum, col := range it.columns {
		var value interface{}
		var err error
		switch col.DType() {
		case IntType:
			value, err = col.IntAt(it.rowNum)
		case FloatType:
			value, err = col.FloatAt(it.rowNum)
		case StringType:
			value, err = col.StringAt(it.rowNum)
		case TimeType:
			value, err = col.TimeAt(it.rowNum)
		case BoolType:
			value, err = col.BoolAt(it.rowNum)
		default:
			err = fmt.Errorf("%s:%d - unknown dtype - %d", col.Name(), it.rowNum, col.DType())
		}

		if err != nil {
			return nil, err
		}

		name := col.Name()
		if name == "" {
			name = it.noNames[colNum]
		}
		row[name] = value
	}

	return row, nil
}
