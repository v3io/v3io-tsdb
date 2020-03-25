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

package chunkenc

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/nuclio/logger"
)

const (
	varTypeNil byte = 0
	// nolint: deadcode,varcheck
	varTypeBlob   byte = 1
	varTypeString byte = 2
	// nolint: deadcode,varcheck
	varTypeBool byte = 3
	// nolint: deadcode,varcheck
	varTypeFloat32 byte = 4
	varTypeFloat64 byte = 5
	// nolint: deadcode,varcheck
	varTypeInt8 byte = 8
	// nolint: deadcode,varcheck
	varTypeInt16 byte = 9
	// nolint: deadcode,varcheck
	varTypeInt32 byte = 10
	// nolint: deadcode,varcheck
	varTypeInt64 byte = 11
)

const (
	varValueNone byte = 0
	// nolint: deadcode,varcheck
	varValueZero byte = 1
	// nolint: deadcode,varcheck
	varValueOnes byte = 2
	varValueAny  byte = 3
)

// Type encoding: 6 bits for var type, 2 bits for predefined type values (e.g. None, zero, NaN, ..)
func decodeType(t byte) (byte, byte)    { return t >> 2, t & 3 }
func encodeType(varType, val byte) byte { return varType<<2 + val&3 }

type VarChunk struct {
	logger logger.Logger

	b       []byte
	samples uint16
	offset  int
}

// NewVarChunk returns a new chunk with variant encoding.
func newVarChunk(logger logger.Logger) Chunk {
	return &VarChunk{logger: logger, b: make([]byte, 0, 1024)}
}

// Encoding returns the encoding type.
func (c *VarChunk) Encoding() Encoding {
	return EncVariant
}

// Bytes returns the underlying byte slice of the chunk.
func (c *VarChunk) Bytes() []byte {
	return c.b
}

func (c *VarChunk) Clear() {
	c.b = c.b[:0]
}

// Appender implements the Chunk interface.
func (c *VarChunk) Appender() (Appender, error) {
	a := &varAppender{logger: c.logger, c: c, samples: &c.samples}
	return a, nil
}

// Iterator implements the Chunk interface.
func (c *VarChunk) Iterator() Iterator {
	return c.iterator()
}

type varAppender struct {
	logger logger.Logger

	c       *VarChunk
	samples *uint16
	t       int64
}

func (a *varAppender) Encoding() Encoding {
	return a.c.Encoding()
}

func (a *varAppender) Chunk() Chunk {
	return a.c
}

func (a *varAppender) Append(t int64, v interface{}) {
	if v == nil {
		a.appendNoValue(t, varTypeNil, varValueNone)
		return
	}

	switch val := v.(type) {
	case string:
		a.appendWithValue(t, varTypeString, []byte(val))

	default:
		a.logger.Error("unsupported type %T of value %v\n", v, v)
	}
}

func (a *varAppender) appendNoValue(t int64, varType, varVal byte) {
	head := uint64(t) & 0x00ffffffffffffff
	head += uint64(encodeType(varType, varVal)) << 56
	appendUint64(&a.c.b, head)
	(*a.samples)++
}

func appendUint64(b *[]byte, v uint64) {
	for i := 0; i < 8; i++ {
		*b = append(*b, byte(v))
		v = v >> 8
	}
}

func (a *varAppender) appendWithUint(t int64, varType byte, val uint64) {
	a.appendNoValue(t, varType, varValueAny)
	appendUint64(&a.c.b, val)
}

func (a *varAppender) appendWithValue(t int64, varType byte, val []byte) {
	a.appendNoValue(t, varType, varValueAny)
	l := uint16(len(val))
	a.c.b = append(a.c.b, byte(l))
	a.c.b = append(a.c.b, byte(l>>8))
	a.c.b = append(a.c.b, val...)
}

func (c *VarChunk) iterator() *varIterator {
	return &varIterator{
		br:       c.b,
		numTotal: c.samples,
	}
}

type varIterator struct {
	br       []byte
	numTotal uint16
	numRead  uint16

	t       int64
	varType byte
	varVal  byte
	val     []byte
	err     error
}

func (it *varIterator) Next() bool {
	if it.err != nil || len(it.br) < 8 {
		return false
	}

	head := binary.LittleEndian.Uint64(it.br[0:8])
	it.varType, it.varVal = decodeType(byte(head >> 56))
	it.t = int64(head & 0x00ffffffffffffff)

	it.br = it.br[8:]

	if it.varType == varTypeFloat64 && it.varVal == varValueAny {

		if len(it.br) < 8 {
			return it.lenError("float64", 8)
		}
		it.val = it.br[0:8]
		it.br = it.br[8:]
	}

	if it.varType == varTypeString && it.varVal == varValueAny {

		if len(it.br) < 2 {
			return it.lenError("var len", 2)
		}
		valLen := int(it.br[1])<<8 + int(it.br[0])

		if len(it.br) < valLen+2 {
			return it.lenError("string", valLen)
		}
		it.val = it.br[2 : valLen+2]
		it.br = it.br[valLen+2:]
	}

	return true
}

func (it *varIterator) lenError(v string, expected int) bool {
	it.err = fmt.Errorf("chunk decoding error, less than %d bytes to store %s value", expected, v)
	return false
}

func (it *varIterator) At() (int64, float64) {

	if it.varType == varTypeFloat64 {
		switch it.varVal {
		case varValueNone:
			return it.t, math.NaN()
		case varValueAny:
			v := binary.LittleEndian.Uint64(it.val)
			return it.t, math.Float64frombits(v)
		}
	}
	return it.t, 0
}

func (it *varIterator) AtString() (int64, string) {

	if it.varType == varTypeFloat64 {
		_, val := it.At()
		return it.t, strconv.FormatFloat(val, 'f', -1, 64)
	}

	return it.t, string(it.val)
}

func (it *varIterator) Err() error {
	return it.err
}
