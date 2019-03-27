package promtsdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/appender"
	"github.com/v3io/v3io-tsdb/pkg/config"
	"github.com/v3io/v3io-tsdb/pkg/pquerier"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

type V3ioPromAdapter struct {
	db     *tsdb.V3ioAdapter
	logger logger.Logger
}

func NewV3ioProm(cfg *config.V3ioConfig, container *v3io.Container, logger logger.Logger) (*V3ioPromAdapter, error) {

	if logger == nil {
		newLogger, err := utils.NewLogger(cfg.LogLevel)
		if err != nil {
			return nil, errors.Wrap(err, "Unable to initialize logger.")
		}
		logger = newLogger
	}

	adapter, err := tsdb.NewV3ioAdapter(cfg, container, logger)
	newAdapter := V3ioPromAdapter{db: adapter, logger: logger.GetChild("v3io-prom-adapter")}
	return &newAdapter, err
}

func (a *V3ioPromAdapter) Appender() (storage.Appender, error) {
	err := a.db.InitAppenderCache()
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
	v3ioQuerier, err := a.db.QuerierV2()
	promQuerier := V3ioPromQuerier{v3ioQuerier: v3ioQuerier, logger: a.logger.GetChild("v3io-prom-query"), mint: mint, maxt: maxt}
	return &promQuerier, err
}

type V3ioPromQuerier struct {
	v3ioQuerier *pquerier.V3ioQuerier
	logger      logger.Logger
	mint, maxt  int64
}

// Select returns a set of series that matches the given label matchers.
func (promQuery *V3ioPromQuerier) Select(params *storage.SelectParams, oms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	name, filter, functions := match2filter(oms, promQuery.logger)
	noAggr := false

	// if a nil params is passed we assume it's a metadata query, so we fetch only the different labelsets withtout data.
	if params == nil {
		labelSets, err := promQuery.v3ioQuerier.GetLabelSets(name, filter)
		if err != nil {
			return nil, nil, err
		}

		return &V3ioPromSeriesSet{newMetadataSeriesSet(labelSets)}, nil, nil
	}

	promQuery.logger.Debug("SelectParams: %+v", params)

	if params.Func != "" {
		// only pass xx_over_time functions (just the xx part)
		// TODO: support count/stdxx, require changes in Prometheus: promql/functions.go, not calc aggregate twice
		if strings.HasSuffix(params.Func, "_over_time") {
			f := params.Func[0:3]
			if params.Step == 0 && (f == "min" || f == "max" || f == "sum" || f == "avg") {
				functions = f
			} else {
				noAggr = true
			}
		}
	}

	selectParams := &pquerier.SelectParams{Name: name,
		Functions: functions,
		Step:      params.Step,
		Filter:    filter,
		From:      promQuery.mint,
		To:        promQuery.maxt}

	set, err := promQuery.v3ioQuerier.SelectProm(selectParams, noAggr)
	return &V3ioPromSeriesSet{s: set}, nil, err
}

// LabelValues returns all potential values for a label name.
func (promQuery *V3ioPromQuerier) LabelValues(name string) ([]string, error) {
	return promQuery.v3ioQuerier.LabelValues(name)
}

func (promQuery *V3ioPromQuerier) LabelNames() ([]string, error) {
	return promQuery.v3ioQuerier.LabelNames()
}

// Close releases the resources of the Querier.
func (promQuery *V3ioPromQuerier) Close() error {
	return nil
}

func match2filter(oms []*labels.Matcher, logger logger.Logger) (string, string, string) {
	var filter []string
	agg := ""
	name := ""

	for _, matcher := range oms {
		logger.Debug("Matcher: %+v", matcher)
		if matcher.Name == aggregate.AggregateLabel {
			agg = matcher.Value
		} else if matcher.Name == "__name__" && matcher.Type == labels.MatchEqual {
			name = matcher.Value
		} else {
			switch matcher.Type {
			case labels.MatchEqual:
				filter = append(filter, fmt.Sprintf("%s=='%s'", matcher.Name, matcher.Value))
			case labels.MatchNotEqual:
				filter = append(filter, fmt.Sprintf("%s!='%s'", matcher.Name, matcher.Value))
			case labels.MatchRegexp:
				filter = append(filter, fmt.Sprintf("regexp_instr(%s,'%s') == 0", matcher.Name, matcher.Value))
			case labels.MatchNotRegexp:
				filter = append(filter, fmt.Sprintf("regexp_instr(%s,'%s') != 0", matcher.Name, matcher.Value))

			}
		}
	}
	filterExp := strings.Join(filter, " and ")
	return name, filterExp, agg
}

type V3ioPromSeriesSet struct {
	s utils.SeriesSet
}

func (s *V3ioPromSeriesSet) Next() bool { return s.s.Next() }
func (s *V3ioPromSeriesSet) Err() error { return s.s.Err() }
func (s *V3ioPromSeriesSet) At() storage.Series {
	series := s.s.At()
	return &V3ioPromSeries{series}
}

// Series represents a single time series.
type V3ioPromSeries struct {
	s utils.Series
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
	s utils.SeriesIterator
}

// Seek advances the iterator forward to the given timestamp.
// If there's no value exactly at t, it advances to the first value
// after t.
func (s *V3ioPromSeriesIterator) Seek(t int64) bool { return s.s.Seek(t) }

// Next advances the iterator by one.
func (s *V3ioPromSeriesIterator) Next() bool { return s.s.Next() }

// At returns the current timestamp/value pair.
func (s *V3ioPromSeriesIterator) At() (t int64, v float64) { return s.s.At() }

// error returns the current error.
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
func (ls Labels) GetKey() (string, string, uint64) {
	key := ""
	name := ""
	for _, lbl := range *ls.lbls {
		if lbl.Name == "__name__" {
			name = lbl.Value
		} else {
			key = key + lbl.Name + "=" + lbl.Value + ","
		}
	}
	if len(key) == 0 {
		return name, "", ls.lbls.Hash()
	}
	return name, key[:len(key)-1], ls.lbls.Hash()

}

// create update expression
func (ls Labels) GetExpr() string {
	lblexpr := ""
	for _, lbl := range *ls.lbls {
		if lbl.Name != "__name__" {
			lblexpr = lblexpr + fmt.Sprintf("%s='%s'; ", lbl.Name, lbl.Value)
		} else {
			lblexpr = lblexpr + fmt.Sprintf("_name='%s'; ", lbl.Value)
		}
	}

	return lblexpr
}

func (ls Labels) LabelNames() []string {
	var res []string
	for _, l := range *ls.lbls {
		res = append(res, l.Name)
	}
	return res
}

func newMetadataSeriesSet(labels []utils.Labels) utils.SeriesSet {
	return &metadataSeriesSet{labels: labels, currentIndex: -1, size: len(labels)}
}

type metadataSeriesSet struct {
	labels       []utils.Labels
	currentIndex int
	size         int
}

func (ss *metadataSeriesSet) Next() bool {
	ss.currentIndex++
	return ss.currentIndex < ss.size
}
func (ss *metadataSeriesSet) At() utils.Series {
	return &metadataSeries{labels: ss.labels[ss.currentIndex]}
}
func (ss *metadataSeriesSet) Err() error {
	return nil
}

type metadataSeries struct {
	labels utils.Labels
}

func (s *metadataSeries) Labels() utils.Labels           { return s.labels }
func (s *metadataSeries) Iterator() utils.SeriesIterator { return utils.NullSeriesIterator{} }
func (s *metadataSeries) GetKey() uint64                 { return s.labels.Hash() }

func (ls Labels) Filter(keep []string) utils.LabelsIfc {
	var res labels.Labels
	for _, l := range *ls.lbls {
		for _, keepLabel := range keep {
			if l.Name == labels.MetricName || l.Name == keepLabel {
				res = append(res, l)
			}
		}
	}
	return Labels{lbls: &res}
}
