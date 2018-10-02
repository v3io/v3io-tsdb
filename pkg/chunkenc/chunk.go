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

The code in this file was largely written by Prometheus Authors as part of
https://github.com/prometheus/prometheus
Copyright 2017 The Prometheus Authors
And is also licensed under the Apache License, Version 2.0;

And was modified to suit Iguazio needs

*/

package chunkenc

import (
	"encoding/binary"
	"fmt"
	"github.com/nuclio/logger"
)

// Encoding is the identifier for a chunk encoding.
type Encoding uint8

func (e Encoding) String() string {
	switch e {
	case EncNone:
		return "none"
	case EncXOR:
		return "XOR"
	}
	return "<unknown>"
}

// The different available chunk encodings.
const (
	EncNone Encoding = 0
	EncXOR  Encoding = 1
)

// Chunk holds a sequence of sample pairs that can be iterated over and appended to.
type Chunk interface {
	Bytes() []byte
	Clear()
	Encoding() Encoding
	Appender() (Appender, error)
	Iterator() Iterator
}

// FromData returns a chunk from a byte slice of chunk data.
func FromData(logger logger.Logger, e Encoding, d []byte, samples uint16) (Chunk, error) {
	switch e {
	case EncXOR:
		return &XORChunk{logger: logger, b: &bstream{count: 0, stream: d}, samples: samples}, nil
	}
	return nil, fmt.Errorf("unknown chunk encoding: %d", e)
}

func ToUint64(bytes []byte) []uint64 {
	array := []uint64{}

	rem := len(bytes) - (len(bytes)/8)*8
	if rem > 0 {
		for b := rem; b < 8; b++ {
			bytes = append(bytes, 0)
		}
	}

	for i := 0; i+8 <= len(bytes); i += 8 {
		val := binary.LittleEndian.Uint64(bytes[i : i+8])
		array = append(array, val)
	}

	return array

}

// Appender adds sample pairs to a chunk.
type Appender interface {
	Append(int64, float64)
	Chunk() Chunk
}

// Iterator is a simple iterator that can only get the next value.
type Iterator interface {
	At() (int64, float64)
	Err() error
	Next() bool
}

// NewNopIterator returns a new chunk iterator that does not hold any data.
func NewNopIterator() Iterator {
	return nopIterator{}
}

type nopIterator struct{}

func (nopIterator) At() (int64, float64) { return 0, 0 }
func (nopIterator) Next() bool           { return false }
func (nopIterator) Err() error           { return nil }
