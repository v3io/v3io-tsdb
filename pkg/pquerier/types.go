package pquerier

import (
	"fmt"
	"strings"

	"github.com/v3io/frames"
	"github.com/v3io/v3io-tsdb/pkg/aggregate"
	"github.com/v3io/v3io-tsdb/pkg/chunkenc"
)

// data and metadata passed to the query processor workers via a channel
type qryResults struct {
	frame    *dataFrame
	query    *partQuery
	name     string
	fields   map[string]interface{}
	encoding chunkenc.Encoding
}

func (q *qryResults) IsRawQuery() bool { return q.frame.isRawSeries }

func (q *qryResults) IsDownsample() bool {
	_, ok := q.frame.columnByName[q.name]

	return ok && q.query.step != 0
}

func (q *qryResults) IsServerAggregates() bool {
	return q.query.aggregationParams != nil && q.query.useServerSideAggregates
}

func (q *qryResults) IsClientAggregates() bool {
	return q.query.aggregationParams != nil && !q.query.useServerSideAggregates
}

type RequestedColumn struct {
	Metric                 string
	Alias                  string
	Function               string
	Interpolator           string
	InterpolationTolerance int64 // tolerance in Millis
}

func (col *RequestedColumn) isCrossSeries() bool {
	return strings.HasSuffix(col.Function, aggregate.CrossSeriesSuffix)
}

// If the function is cross series, remove the suffix otherwise leave it as is
func (col *RequestedColumn) GetFunction() string {
	return strings.TrimSuffix(col.Function, aggregate.CrossSeriesSuffix)
}

func (col *RequestedColumn) GetColumnName() string {
	if col.Alias != "" {
		return col.Alias
	}
	// If no aggregations are requested (raw down sampled data)
	if col.Function == "" {
		return col.Metric
	}
	return fmt.Sprintf("%v(%v)", col.Function, col.Metric)
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
func (c *columnMeta) isWildcard() bool { return c.metric == "" }

// Concrete Column = has real data behind it, Virtual column = described as a function on top of concrete columns
func (c columnMeta) isConcrete() bool { return c.function == 0 || aggregate.IsRawAggregate(c.function) }
func (c columnMeta) getColumnName() string {
	if c.alias != "" {
		return c.alias
	}
	// If no aggregations are requested (raw down sampled data)
	if c.function == 0 {
		return c.metric
	}
	return fmt.Sprintf("%v(%v)", c.function.String(), c.metric)
}

// SeriesSet contains a set of series.
type FrameSet interface {
	NextFrame() bool
	GetFrame() (frames.Frame, error)
	Err() error
}

// Null-frame set
type nullFrameSet struct {
	err error
}

func (s nullFrameSet) NextFrame() bool                 { return false }
func (s nullFrameSet) GetFrame() (frames.Frame, error) { return nil, nil }
func (s nullFrameSet) Err() error                      { return s.err }
