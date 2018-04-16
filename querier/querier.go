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

package querier

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/aggregate"
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-tsdb/partmgr"
	"github.com/v3io/v3io-tsdb/v3ioutil"
	"strings"
)

func NewV3ioQuerier(container *v3io.Container, logger logger.Logger, mint, maxt int64,
	keymap *map[string]bool, cfg *config.TsdbConfig, partMngr *partmgr.PartitionManager) V3ioQuerier {
	newQuerier := V3ioQuerier{container: container, mint: mint, maxt: maxt,
		logger: logger.GetChild("Querier"), Keymap: keymap, cfg: cfg}
	newQuerier.partitionMngr = partMngr
	return newQuerier
}

type V3ioQuerier struct {
	logger        logger.Logger
	container     *v3io.Container
	cfg           *config.TsdbConfig
	mint, maxt    int64
	Keymap        *map[string]bool
	partitionMngr *partmgr.PartitionManager
}

func (q V3ioQuerier) Select(params *storage.SelectParams, oms ...*labels.Matcher) (storage.SeriesSet, error) {
	filter := match2filter(oms)

	newAggrSeries, err := aggregate.NewAggregateSeries(params.Func, "v")
	if err != nil {
		return nil, err
	}

	mint, maxt := q.mint, q.maxt
	if q.partitionMngr.IsCyclic() {
		partition := q.partitionMngr.GetHead()
		mint = partition.CyclicMinTime(mint, maxt)
		q.logger.DebugWith("Select - new cyclic series", "from", mint, "to", maxt, "filter", filter)
		newSet := &seriesSet{mint: mint, maxt: maxt, partition: partition, logger: q.logger}

		if newAggrSeries != nil && params.Step != 0 {
			newSet.aggrSeries = newAggrSeries
			newSet.interval = params.Step
		}

		err := newSet.getItems(q.cfg.Path+"/", filter, q.container)
		if err != nil {
			return nil, err
		}

		return newSet, nil

	}

	q.logger.Warn("Select with multi partitions, not supported ")
	// TODO: support multiple partitions
	//partitions := q.partitionMngr.PartsForRange(q.mint, q.maxt)

	return nullSeriesSet{}, nil
}

func match2filter(oms []*labels.Matcher) string {
	filter := []string{}
	//name := ""
	for _, matcher := range oms {
		if matcher.Name == "__name__" {
			//name = matcher.Value
			filter = append(filter, fmt.Sprintf("_name=='%s'", matcher.Value))
		} else {
			switch matcher.Type {
			case labels.MatchEqual:
				filter = append(filter, fmt.Sprintf("%s=='%s'", matcher.Name, matcher.Value))
			case labels.MatchNotEqual:
				filter = append(filter, fmt.Sprintf("%s!='%s'", matcher.Name, matcher.Value))

			}
		}
	}
	return strings.Join(filter, " and ")
}

func (q V3ioQuerier) LabelValues(name string) ([]string, error) {
	list := []string{}
	for k, _ := range *q.Keymap {
		list = append(list, k)
	}
	return list, nil
}

func (q V3ioQuerier) Close() error {
	return nil
}

func newSeriesSet(partition *partmgr.DBPartition, mint, maxt int64) *seriesSet {

	return &seriesSet{mint: mint, maxt: maxt, partition: partition}
}

type seriesSet struct {
	err        error
	logger     logger.Logger
	partition  *partmgr.DBPartition
	iter       *v3ioutil.V3ioItemsCursor
	mint, maxt int64
	attrs      []string
	chunkIds   []int

	interval   int64
	aggrSeries *aggregate.AggregateSeries
	aggrIdx    int
	currSeries storage.Series
	aggrSet    *aggregate.AggregateSet
	baseTime   int64
}

// TODO: get items per partition + merge, per partition calc attrs
func (s *seriesSet) getItems(path, filter string, container *v3io.Container) error {

	attrs := []string{"_lset", "_meta", "_name", "_maxtime"}
	s.attrs, s.chunkIds = s.partition.Range2Attrs("v", s.mint, s.maxt)
	attrs = append(attrs, s.attrs...)

	s.logger.DebugWith("Select - GetItems", "path", path, "attr", attrs, "filter", filter)
	input := v3io.GetItemsInput{Path: path, AttributeNames: attrs, Filter: filter}
	iter, err := v3ioutil.NewItemsCursor(container, &input)
	if err != nil {
		return err
	}

	s.iter = iter
	return nil

}

func (s seriesSet) Next() bool {
	if s.aggrSeries == nil {
		if s.iter.Next() {
			s.currSeries = NewSeries(&s)
			return true
		}
		return false
	}

	if s.aggrIdx == s.aggrSeries.NumFunctions()-1 {
		if !s.iter.Next() {
			return false
		}
		s.currSeries = NewSeries(&s)
		s.chunks2Aggregates()
	}

	s.aggrIdx = (s.aggrIdx + 1) % s.aggrSeries.NumFunctions()
	return true
}

func (s seriesSet) chunks2Aggregates() {

	s.aggrSet = s.aggrSeries.NewAggregateSet(24)

	iter := s.currSeries.Iterator()
	if iter.Next() {

		if iter.Err() != nil {
			s.err = iter.Err()
			return
		}

		t0, _ := iter.At()
		s.baseTime = (t0 / s.interval) * s.interval

		for {
			t, v := iter.At()
			s.aggrSet.AppendCell(int((t-s.baseTime)/s.interval), v)
			if !iter.Next() {
				s.err = iter.Err()
				break
			}
		}
	}
}

func (s seriesSet) Err() error {
	if s.iter.Err() != nil {
		return s.iter.Err()
	}
	return s.err
}

func (s seriesSet) At() storage.Series {
	if s.aggrSeries == nil {
		return s.currSeries
	}

	return NewAggrSeries(&s, s.aggrSeries.GetFunctions()[s.aggrIdx])
}

type nullSeriesSet struct {
	err error
}

func (s nullSeriesSet) Next() bool         { return false }
func (s nullSeriesSet) At() storage.Series { return nil }
func (s nullSeriesSet) Err() error         { return s.err }

func mergeLables(a, b []string) []string {
	maxl := len(a)
	if len(b) > len(a) {
		maxl = len(b)
	}
	res := make([]string, 0, maxl*10/9)

	for len(a) > 0 && len(b) > 0 {
		d := strings.Compare(a[0], b[0])

		if d == 0 {
			res = append(res, a[0])
			a, b = a[1:], b[1:]
		} else if d < 0 {
			res = append(res, a[0])
			a = a[1:]
		} else if d > 0 {
			res = append(res, b[0])
			b = b[1:]
		}
	}

	// Append all remaining elements.
	res = append(res, a...)
	res = append(res, b...)
	return res
}
