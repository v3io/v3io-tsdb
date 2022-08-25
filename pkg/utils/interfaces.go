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
