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

package tsdb

import (
	"time"

	"github.com/pkg/errors"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	tsdbutils "github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
)

type tsdbIteratorOld struct {
	request     *frames.ReadRequest
	set         tsdbutils.SeriesSet
	err         error
	withColumns bool
	currFrame   frames.Frame
}

type tsdbIterator struct {
	request     *frames.ReadRequest
	set         pquerier.FrameSet
	err         error
	withColumns bool
	currFrame   frames.Frame
}

func (b *Backend) Read(request *frames.ReadRequest) (frames.FrameIterator, error) {

	step, err := tsdbutils.Str2duration(request.Step)
	if err != nil {
		return nil, err
	}

	// TODO: start & end times
	to := time.Now().Unix() * 1000
	if request.End != "" {
		to, err = tsdbutils.Str2unixTime(request.End)
		if err != nil {
			return nil, err
		}
	}

	from := to - 1000*3600 // default of last hour
	if request.Start != "" {
		from, err = tsdbutils.Str2unixTime(request.Start)
		if err != nil {
			return nil, err
		}
	}

	b.logger.DebugWith("Query", "from", from, "to", to, "table", request.Table,
		"filter", request.Filter, "functions", request.Aggragators, "step", step)

	table := request.Table
	var selectParams *pquerier.SelectParams
	if request.Query != "" {
		selectParams, table, err = pquerier.ParseQuery(request.Query)
		if err != nil {
			return nil, err
		}
	}
	adapter, err := b.GetAdapter(request.Session, table)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create adapter")
	}

	qry, err := adapter.QuerierV2()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize Querier")
	}

	iter := tsdbIterator{request: request}
	name := ""
	if len(request.Columns) > 0 {
		name = strings.Join(request.Columns, ",")
		iter.withColumns = true
	}

	if selectParams != nil {
		selectParams.From = from
		selectParams.To = to
		selectParams.Step = step
	} else {
		selectParams = &pquerier.SelectParams{Name: name,
			From:      from,
			To:        to,
			Step:      step,
			Functions: request.Aggragators,
			Filter:    request.Filter,
			GroupBy:   request.GroupBy}
	}

	iter.set, err = qry.SelectDataFrame(selectParams)
	if err != nil {
		return nil, errors.Wrap(err, "Failed on TSDB Select")
	}

	return &iter, nil
}

func oldQuery(adapter *tsdb.V3ioAdapter, request *frames.ReadRequest, from, to, step int64) (frames.FrameIterator, error) {
	qry, err := adapter.Querier(nil, from, to)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize Querier")
	}

	iter := tsdbIteratorOld{request: request}
	name := ""
	if len(request.Columns) > 0 {
		name = request.Columns[0]
		iter.withColumns = true
	}

	iter.set, err = qry.Select(name, request.Aggragators, step, request.Filter)
	if err != nil {
		return nil, errors.Wrap(err, "Failed on TSDB Select")
	}

	return &iter, nil
}

func (i *tsdbIterator) Next() bool {
	if i.set.NextFrame() {
		frame, err := i.set.GetFrame()
		if err != nil {
			i.err = err
			return false
		}
		labels := map[string]interface{}{}

		columns := make([]frames.Column, len(frame.Names()))
		indices := frame.Indices()
		for i, colName := range frame.Names() {
			columns[i], _ = frame.Column(colName) // Because we are iterating over the Names() it is safe to discard the error
		}

		for labelName, labelValue := range frame.Labels() {
			name := labelName
			if name == config.PrometheusMetricNameAttribute {
				name = "metric_name"
			}

			labels[name] = labelValue
			icol, err := frames.NewLabelColumn(name, labelValue, frame.Len())
			if err != nil {
				i.err = err
				return false
			}

			if i.request.MultiIndex {
				indices = append(indices, icol)
			} else {
				columns = append(columns, icol)
			}
		}

		i.currFrame, err = frames.NewFrame(columns, indices, labels)
		if err != nil {
			i.err = err
			return false
		}

		return true
	}

	if i.set.Err() != nil {
		i.err = i.set.Err()
	}

	return false
}

func (i *tsdbIterator) Err() error {
	return i.err
}

func (i *tsdbIterator) At() frames.Frame {
	return i.currFrame
}

func (i *tsdbIteratorOld) Next() bool {
	if i.set.Next() {
		series := i.set.At()
		labels := map[string]interface{}{}
		values := []float64{}
		times := []time.Time{}

		iter := series.Iterator()
		for iter.Next() {
			t, v := iter.At()
			values = append(values, v)
			times = append(times, time.Unix(t/1000, (t%1000)*1000))
		}

		if iter.Err() != nil {
			i.err = iter.Err()
			return false
		}

		timeCol, err := frames.NewSliceColumn("Date", times)
		if err != nil {
			i.err = err
			return false
		}

		colname := "values"
		valCol, err := frames.NewSliceColumn(colname, values)
		if err != nil {
			i.err = err
			return false
		}

		columns := []frames.Column{valCol}
		indices := []frames.Column{timeCol}
		for _, v := range series.Labels() {
			name := v.Name
			if v.Name == "__name__" {
				name = "metric_name"
			}

			labels[name] = v.Value
			icol, err := frames.NewLabelColumn(name, v.Value, len(values))
			if err != nil {
				i.err = err
				return false
			}

			if i.request.MultiIndex {
				indices = append(indices, icol)
			} else {
				columns = append(columns, icol)
			}
		}

		i.currFrame, err = frames.NewFrame(columns, indices, labels)
		if err != nil {
			i.err = err
			return false
		}

		return true
	}

	if i.set.Err() != nil {
		i.err = i.set.Err()
	}

	return false
}

func (i *tsdbIteratorOld) Err() error {
	return i.err
}

func (i *tsdbIteratorOld) At() frames.Frame {
	return i.currFrame
}
