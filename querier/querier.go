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
	"github.com/v3io/v3io-tsdb/v3ioutil"
	"strings"
)

func NewV3ioQuerier(container *v3io.Container, logger logger.Logger, mint, maxt int64,
	keymap *map[string]bool, cfg *config.TsdbConfig) V3ioQuerier {
	return V3ioQuerier{container: container, mint: mint, maxt: maxt, logger: logger, Keymap: keymap, cfg: cfg}
}

type V3ioQuerier struct {
	logger     logger.Logger
	container  *v3io.Container
	cfg        *config.TsdbConfig
	mint, maxt int64
	Keymap     *map[string]bool
}

func (q V3ioQuerier) Select(oms ...*labels.Matcher) (storage.SeriesSet, error) {
	filter := match2filter(oms)
	vc := v3ioutil.NewV3ioClient(q.logger, q.Keymap)
	iter, err := vc.GetItems(q.container, q.cfg.Path+"/", filter, []string{"*"})
	if err != nil {
		return nil, err
	}

	newSeriesSet := seriesSet{iter: iter, mint: q.mint, maxt: q.maxt}
	return newSeriesSet, nil
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

type seriesSet struct {
	iter       *v3ioutil.V3ioItemsCursor
	mint, maxt int64
}

func (s seriesSet) Next() bool { return s.iter.Next() }
func (s seriesSet) Err() error { return s.iter.Err() }

func (s seriesSet) At() storage.Series {
	return series{lset: s.iter.GetLables(), iter: s.iter.GetSeriesIter(s.mint, s.maxt)}
}

type series struct {
	lset labels.Labels
	iter SeriesIterator
}

func (s series) Labels() labels.Labels            { return s.lset }
func (s series) Iterator() storage.SeriesIterator { return s.iter }

// SeriesIterator iterates over the data of a time series.
type SeriesIterator interface {
	// Seek advances the iterator forward to the given timestamp.
	// If there's no value exactly at t, it advances to the first value
	// after t.
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	At() (t int64, v float64)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}
