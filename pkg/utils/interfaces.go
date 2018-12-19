package utils

import "github.com/v3io/v3io-tsdb/pkg/chunkenc"

// SeriesSet contains a set of series.
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

// Null-series set
type NullSeriesSet struct {
	err error
}

func (s NullSeriesSet) Next() bool { return false }
func (s NullSeriesSet) At() Series { return nil }
func (s NullSeriesSet) Err() error { return s.err }

// Series represents a single time series.
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	Labels() Labels
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
	// At returns the current timestamp/String value pair.
	AtString() (t int64, v string)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
	// Encoding returns the encoding of the data. according to the encoding you will call the appropriate At method
	Encoding() chunkenc.Encoding
}

// Null-series iterator
type NullSeriesIterator struct {
	err error
}

func (s NullSeriesIterator) Seek(t int64) bool             { return false }
func (s NullSeriesIterator) Next() bool                    { return false }
func (s NullSeriesIterator) At() (t int64, v float64)      { return 0, 0 }
func (s NullSeriesIterator) AtString() (t int64, v string) { return 0, "" }
func (s NullSeriesIterator) Err() error                    { return s.err }
func (s NullSeriesIterator) Encoding() chunkenc.Encoding   { return chunkenc.EncNone }
