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
	"github.com/v3io/v3io-tsdb/config"
	"github.com/v3io/v3io-tsdb/utils"
	"github.com/v3io/v3io-tsdb/v3ioutil"
	"strings"
)

func NewV3ioQuerier(container *v3io.Container, logger logger.Logger, mint, maxt int64,
	keymap *map[string]bool, cfg *config.TsdbConfig) V3ioQuerier {
	newQuerier := V3ioQuerier{container: container, mint: mint, maxt: maxt,
		logger: logger, Keymap: keymap, cfg: cfg}
	newQuerier.headPartition = utils.NewColDBPartition(cfg)
	return newQuerier
}

type V3ioQuerier struct {
	logger        logger.Logger
	container     *v3io.Container
	cfg           *config.TsdbConfig
	mint, maxt    int64
	Keymap        *map[string]bool
	headPartition *utils.ColDBPartition
}

func (q V3ioQuerier) Select(params *storage.SelectParams, oms ...*labels.Matcher) (storage.SeriesSet, error) {
	fmt.Println("Select:", params)
	filter := match2filter(oms)
	//vc := v3ioutil.NewV3ioClient(q.logger, q.Keymap)
	//iter, err := vc.GetItems(q.container, q.cfg.Path+"/", filter, attrs)

	newSet := newSeriesSet(q.headPartition, q.mint, q.maxt)
	err := newSet.getItems(q.cfg.Path+"/", filter, q.container)
	if err != nil {
		return nil, err
	}

	return newSet, nil
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
	fmt.Println("Get LabelValues:", name)
	list := []string{}
	for k, _ := range *q.Keymap {
		list = append(list, k)
	}
	return list, nil
}

func (q V3ioQuerier) Close() error {
	return nil
}

func newSeriesSet(partition *utils.ColDBPartition, mint, maxt int64) seriesSet {

	pmin, pmax := partition.PartTimeRange()
	if pmin > mint {
		mint = pmin
	}
	if pmax < maxt {
		maxt = pmax
	}

	return seriesSet{mint: mint, maxt: maxt, partition: partition}
}

type seriesSet struct {
	partition  *utils.ColDBPartition
	iter       *v3ioutil.V3ioItemsCursor
	mint, maxt int64
	attrs      []string
	chunkIds   []int
}

// TODO: get items per partition + merge, per partition calc attrs
func (s seriesSet) getItems(path, filter string, container *v3io.Container) error {

	attrs := []string{"_lset", "_meta_v", "_name", "_maxtime"}
	s.attrs, s.chunkIds = utils.Range2Attrs("v", 0, s.mint, s.maxt)
	attrs = append(attrs, s.attrs...)
	input := v3io.GetItemsInput{Path: path, AttributeNames: attrs, Filter: filter}

	response, err := container.Sync.GetItems(&input)
	if err != nil {
		return err
	}

	s.iter = v3ioutil.NewItemsCursor(container, &input, response)
	return nil

}

func (s seriesSet) Next() bool { return s.iter.Next() }
func (s seriesSet) Err() error { return s.iter.Err() }

func (s seriesSet) At() storage.Series {
	return NewSeries(&s)
}

type errSeriesSet struct {
	err error
}

func (s errSeriesSet) Next() bool         { return false }
func (s errSeriesSet) At() storage.Series { return nil }
func (s errSeriesSet) Err() error         { return s.err }

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
