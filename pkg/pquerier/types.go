package pquerier

import (
	"fmt"

	"github.com/v3io/v3io-tsdb/pkg/aggregate"
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
