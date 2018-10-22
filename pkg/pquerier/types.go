package pquerier

import (
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

// columns metadata for applying various functions and grouping on data WIP
type functionType int16

type columnMeta struct {
	alias          string
	function       functionType
	functionParams []interface{}
	interpolator   int8
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
