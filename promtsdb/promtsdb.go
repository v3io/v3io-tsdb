package promtsdb

import (
	"context"
	"fmt"
	"github.com/nuclio/logger"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/appender"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/querier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"strings"
)

type V3ioPromAdapter struct {
	db *tsdb.V3ioAdapter
}

func NewV3ioProm(cfg *config.V3ioConfig, container *v3io.Container, logger logger.Logger) (*V3ioPromAdapter, error) {

	adapter, err := tsdb.NewV3ioAdapter(cfg, container, logger)
	newAdapter := V3ioPromAdapter{db: adapter}
	return &newAdapter, err
}

func (a *V3ioPromAdapter) Appender() (storage.Appender, error) {
	err := a.db.MetricsCache.StartIfNeeded()
	if err != nil {
		return nil, err
	}

	newAppender := v3ioAppender{metricsCache: a.db.MetricsCache}
	return newAppender, nil
}

func (a *V3ioPromAdapter) StartTime() (int64, error) {
	return a.db.StartTime()
}

func (a *V3ioPromAdapter) Close() error {
	return nil
}

func (a *V3ioPromAdapter) Querier(_ context.Context, mint, maxt int64) (storage.Querier, error) {
	v3ioQuerier, err := a.db.Querier(nil, mint, maxt)
	querier := V3ioPromQuerier{q: v3ioQuerier}
	return &querier, err
}

type V3ioPromQuerier struct {
	q *querier.V3ioQuerier
}

// Select returns a set of series that matches the given label matchers.
func (q *V3ioPromQuerier) Select(params *storage.SelectParams, oms ...*labels.Matcher) (storage.SeriesSet, error) {
	name, filter, functions := match2filter(oms)
	if params.Func != "" {
		functions = params.Func
	}
	set, err := q.q.Select(name, functions, params.Step, filter)
	return &V3ioPromSeriesSet{s: set}, err
}

// LabelValues returns all potential values for a label name.
func (q *V3ioPromQuerier) LabelValues(name string) ([]string, error) {
	return q.q.LabelValues(name)
}

// Close releases the resources of the Querier.
func (q *V3ioPromQuerier) Close() error {
	return nil
}

func match2filter(oms []*labels.Matcher) (string, string, string) {
	filter := []string{}
	aggregator := ""
	name := ""

	for _, matcher := range oms {
		if matcher.Name == "Aggregator" {
			aggregator = matcher.Value
		} else if matcher.Name == "__name__" && matcher.Type == labels.MatchEqual {
			name = matcher.Value
		} else {
			switch matcher.Type {
			case labels.MatchEqual:
				filter = append(filter, fmt.Sprintf("%s=='%s'", matcher.Name, matcher.Value))
			case labels.MatchNotEqual:
				filter = append(filter, fmt.Sprintf("%s!='%s'", matcher.Name, matcher.Value))

			}
		}
	}
	return name, strings.Join(filter, " and "), aggregator
}

type V3ioPromSeriesSet struct {
	s querier.SeriesSet
}

func (s *V3ioPromSeriesSet) Next() bool { return s.s.Next() }
func (s *V3ioPromSeriesSet) Err() error { return s.s.Err() }
func (s *V3ioPromSeriesSet) At() storage.Series {
	series := s.s.At()
	return &V3ioPromSeries{series}
}

// Series represents a single time series.
type V3ioPromSeries struct {
	s querier.Series
}

// Labels returns the complete set of labels identifying the series.
func (s *V3ioPromSeries) Labels() labels.Labels {
	lbls := labels.Labels{}
	for _, l := range s.s.Labels() {
		lbls = append(lbls, labels.Label{Name: l.Name, Value: l.Value})
	}

	return lbls
}

// Iterator returns a new iterator of the data of the series.
func (s *V3ioPromSeries) Iterator() storage.SeriesIterator {
	return &V3ioPromSeriesIterator{s: s.s.Iterator()}
}

// SeriesIterator iterates over the data of a time series.
type V3ioPromSeriesIterator struct {
	s querier.SeriesIterator
}

// Seek advances the iterator forward to the given timestamp.
// If there's no value exactly at t, it advances to the first value
// after t.
func (s *V3ioPromSeriesIterator) Seek(t int64) bool { return s.s.Seek(t) }

// Next advances the iterator by one.
func (s *V3ioPromSeriesIterator) Next() bool { return s.s.Next() }

// At returns the current timestamp/value pair.
func (s *V3ioPromSeriesIterator) At() (t int64, v float64) { return s.s.At() }

// Err returns the current error.
func (s *V3ioPromSeriesIterator) Err() error { return s.s.Err() }

type v3ioAppender struct {
	metricsCache *appender.MetricsCache
}

func (a v3ioAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	lbls := Labels{lbls: &lset}
	return a.metricsCache.Add(lbls, t, v)
}

func (a v3ioAppender) AddFast(lset labels.Labels, ref uint64, t int64, v float64) error {
	return a.metricsCache.AddFast(ref, t, v)
}

func (a v3ioAppender) Commit() error   { return nil }
func (a v3ioAppender) Rollback() error { return nil }

type Labels struct {
	lbls *labels.Labels
}

// convert Label set to a string in the form key1=v1,key2=v2.. + name + hash
func (l Labels) GetKey() (string, string, uint64) {
	key := ""
	name := ""
	for _, lbl := range *l.lbls {
		if lbl.Name == "__name__" {
			name = lbl.Value
		} else {
			key = key + lbl.Name + "=" + lbl.Value + ","
		}
	}
	if len(key) == 0 {
		return name, "", l.lbls.Hash()
	}
	return name, key[:len(key)-1], l.lbls.Hash()

}

// create update expression
func (l Labels) GetExpr() string {
	lblexpr := ""
	for _, lbl := range *l.lbls {
		if lbl.Name != "__name__" {
			lblexpr = lblexpr + fmt.Sprintf("%s='%s'; ", lbl.Name, lbl.Value)
		} else {
			lblexpr = lblexpr + fmt.Sprintf("_name='%s'; ", lbl.Value)
		}
	}

	return lblexpr
}
