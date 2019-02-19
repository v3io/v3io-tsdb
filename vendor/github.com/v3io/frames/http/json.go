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

package http

import (
	"time"

	"github.com/pkg/errors"

	"github.com/v3io/frames"
)

// JSONColumn is JSON representation of a column
type JSONColumn struct {
	Name string      `json:"name"`
	Data interface{} `json:"data"`
}

// JSONFrame is JSON representation of a frame
type JSONFrame struct {
	Columns []*JSONColumn `json:"columns"`
	Indices []*JSONColumn `json:"indices"`
}

func frameToJSON(frame frames.Frame) (*JSONFrame, error) {
	columns := make([]*JSONColumn, len(frame.Names()))
	var err error
	for i, name := range frame.Names() {
		col, err := frame.Column(name)
		if err != nil {
			return nil, err
		}

		columns[i], err = columnToJSON(col)
		if err != nil {
			return nil, errors.Wrapf(err, "can't convert column %q to JSON", name)
		}
	}

	indices := make([]*JSONColumn, len(frame.Indices()))
	for i, col := range frame.Indices() {
		indices[i], err = columnToJSON(col)
		if err != nil {
			return nil, errors.Wrapf(err, "can't convert index %q to JSON", col.Name())
		}
	}

	return &JSONFrame{columns, indices}, nil
}

func columnToJSON(col frames.Column) (*JSONColumn, error) {
	var data interface{}
	var err error

	switch col.DType() {
	case frames.BoolType:
		data, err = col.Bools()
	case frames.FloatType:
		data, err = col.Floats()
	case frames.IntType:
		data, err = col.Ints()
	case frames.StringType:
		data = col.Strings()
	case frames.TimeType:
		data, err = col.Times()
		if err == nil {
			tdata := data.([]time.Time)
			ts := make([]int64, len(tdata))
			for i, t := range tdata {
				ts[i] = t.UnixNano()
			}
			data = ts
		}
	}

	if err != nil {
		return nil, err
	}

	jcol := &JSONColumn{
		Name: col.Name(),
		Data: data,
	}
	return jcol, nil
}
