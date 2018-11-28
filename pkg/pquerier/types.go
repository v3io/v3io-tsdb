package pquerier

import (
	"fmt"

	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

// data and metadata passed to the query processor workers via a channel
type qryResults struct {
	frame    *dataFrame
	query    *partQuery
	name     string
	fields   map[string]interface{}
	encoding int16
}

func (q *qryResults) IsRawQuery() bool { return q.frame.isRawSeries }

func (q *qryResults) IsDownsample() bool {
	_, ok := q.frame.columnByName[q.name]

	return ok && q.query.step != 0
}

func (q *qryResults) IsServerAggregates() bool {
	return q.query.aggregationParams != nil && q.query.aggregationParams.CanAggregate(q.query.partition.AggrType())
}

func (q *qryResults) IsClientAggregates() bool {
	return q.query.aggregationParams != nil && !q.query.aggregationParams.CanAggregate(q.query.partition.AggrType())
}

type RequestedColumn struct {
	Metric                 string
	Alias                  string
	Function               string
	Interpolator           string
	InterpolationTolerance int64 // tolerance in Millis
}

type columnMeta struct {
	metric                 string
	alias                  string
	function               aggregate.AggrType
	functionParams         []interface{}
	interpolationType      InterpolationType
	interpolationTolerance int64
	isHidden               bool // real columns = columns the user has specifically requested. Hidden columns = columns needed to calculate the real columns but don't show to the user
}

// if a user specifies he wants all metrics
func (c *columnMeta) isWildcard() bool { return c.metric == "*" }

// Concrete Column = has real data behind it, Virtual column = described as a function on top of concrete columns
func (c columnMeta) isConcrete() bool { return c.function == 0 || aggregate.IsRawAggregate(c.function) }
func (c columnMeta) getColumnName() string {
	// If no aggregations are requested (raw down sampled data)
	if c.function == 0 {
		return c.metric
	}
	return fmt.Sprintf("%v(%v)", c.function.String(), c.metric)
}

// SeriesSet contains a set of series.
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

// Null-series set
type nullSeriesSet struct {
	err error
}

func (s nullSeriesSet) Next() bool { return false }
func (s nullSeriesSet) At() Series { return nil }
func (s nullSeriesSet) Err() error { return s.err }

// SeriesSet contains a set of series.
type FrameSet interface {
	NextFrame() bool
	GetFrame() *dataFrame
	Err() error
}

// Null-frame set
type nullFrameSet struct {
	err error
}

func (s nullFrameSet) NextFrame() bool      { return false }
func (s nullFrameSet) GetFrame() *dataFrame { return nil }
func (s nullFrameSet) Err() error           { return s.err }

// Series represents a single time series.
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	Labels() utils.Labels
	// Iterator returns a new iterator of the data of the series.
	Iterator() SeriesIterator
	// Unique key for sorting
	GetKey() uint64
}

// SeriesIterator iterates over the data of a time series.
type SeriesIterator interface {
	// Seek advances the iterator forward to the given timestamp.
	// If there's no value exactly at t, it advances to the first value after t.
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	At() (t int64, v float64)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}
