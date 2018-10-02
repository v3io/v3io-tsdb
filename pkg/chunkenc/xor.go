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

The code in this file was largely written by Damian Gryski as part of
https://github.com/dgryski/go-tsz and published under the license below.
and was later on modified by the Prometheus project in
https://github.com/prometheus/prometheus
Which are licensed under the Apache License, Version 2.0 (the "License");

Followed by modifications found here to suit Iguazio needs

Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package chunkenc

import (
	"github.com/nuclio/logger"
	"math"
	"math/bits"
)

// XORChunk holds XOR encoded sample data.
type XORChunk struct {
	logger logger.Logger

	b       *bstream
	samples uint16
	offset  int
}

// NewXORChunk returns a new chunk with XOR encoding of the given size.
func NewXORChunk(logger logger.Logger) Chunk {
	//b := make([]byte, 32, 32)
	return &XORChunk{logger: logger, b: newBWriter(256)}
}

// Encoding returns the encoding type.
func (c *XORChunk) Encoding() Encoding {
	return EncXOR
}

// Bytes returns the underlying byte slice of the chunk.
func (c *XORChunk) Bytes() []byte {
	//return c.b.getbytes()
	return c.b.bytes()
}

func (c *XORChunk) Clear() {
	//c.b.rptr = c.b.getLen()
	c.b.clear()
}

// Appender implements the Chunk interface.
// new implementation, doesnt read the existing buffer, assume its new
func (c *XORChunk) Appender() (Appender, error) {
	a := &xorAppender{logger: c.logger, c: c, b: c.b, samples: &c.samples}
	if c.samples == 0 {
		a.leading = 0xff
	}
	return a, nil
}

/* old Appender TODO: do we need to append to existing buffer? maybe in stateless/slow clients
func (c *XORChunk) aAppender() (Appender, error) {
	it := c.iterator()

	// To get an appender we must know the state it would have if we had
	// appended all existing data from scratch.
	// We iterate through the end and populate via the iterator's state.
	for it.Next() {
	}
	if err := it.error(); err != nil {
		return nil, err
	}

	a := &xorAppender{
		c:        c,
		b:        c.b,
		samples:  &c.samples,
		t:        it.t,
		v:        it.val,
		tDelta:   it.tDelta,
		leading:  it.leading,
		trailing: it.trailing,
	}
	if c.samples == 0 {
		a.leading = 0xff
	}
	return a, nil
}
*/

func (c *XORChunk) iterator() *xorIterator {
	// Should iterators guarantee to act on a copy of the data so it doesn't lock append?
	// When using striped locks to guard access to chunks, probably yes.
	// Could only copy data if the chunk is not completed yet.
	return &xorIterator{
		br:       newBReader(c.b.bytes()), // TODO: may need merge
		numTotal: c.samples,
	}
}

// Iterator implements the Chunk interface.
func (c *XORChunk) Iterator() Iterator {
	return c.iterator()
}

type xorAppender struct {
	logger logger.Logger

	c       *XORChunk
	b       *bstream
	samples *uint16

	t      int64
	v      float64
	tDelta uint64

	leading  uint8
	trailing uint8
}

func (a *xorAppender) Chunk() Chunk {
	return a.c
}

func (a *xorAppender) Append(t int64, v float64) {
	var tDelta uint64
	num := *a.samples

	// Do not append if sample is too old.
	if t < a.t {
		a.logger.Info("Discarding sample from %d, as it is older than the latest sample (%d).", t, a.t)
		return
	}

	if num == 0 {
		// add a signature 11111 to indicate start of cseries in case we put few in the same chunk (append to existing)
		a.b.writeBits(0x1f, 5)
		a.b.writeBits(uint64(t), 51)
		a.b.writeBits(math.Float64bits(v), 64)

	} else if num == 1 {
		tDelta = uint64(t - a.t)

		a.b.writeBits(tDelta, 32)
		a.writeVDelta(v)

	} else {
		tDelta = uint64(t - a.t)
		dod := int64(tDelta - a.tDelta)

		// Gorilla has a max resolution of seconds, Prometheus milliseconds.
		// Thus we use higher value range steps with larger bit size.
		switch {
		case dod == 0:
			a.b.writeBit(zero)
		case bitRange(dod, 14):
			a.b.writeBits(0x02, 2) // '10'
			a.b.writeBits(uint64(dod), 14)
		case bitRange(dod, 17):
			a.b.writeBits(0x06, 3) // '110'
			a.b.writeBits(uint64(dod), 17)
		case bitRange(dod, 20):
			a.b.writeBits(0x0e, 4) // '1110'
			a.b.writeBits(uint64(dod), 20)
		default:
			a.b.writeBits(0x1e, 5) // '11110'
			a.b.writeBits(uint64(dod), 32)
		}

		a.writeVDelta(v)
	}

	a.t = t
	a.v = v
	(*a.samples)++
	a.tDelta = tDelta

	a.b.padToByte()
}

func bitRange(x int64, nbits uint8) bool {
	return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}

func (a *xorAppender) writeVDelta(v float64) {
	vDelta := math.Float64bits(v) ^ math.Float64bits(a.v)

	if vDelta == 0 {
		a.b.writeBit(zero)
		return
	}
	a.b.writeBit(one)

	leading := uint8(bits.LeadingZeros64(vDelta))
	trailing := uint8(bits.TrailingZeros64(vDelta))

	// Clamp number of leading zeros to avoid overflow when encoding.
	if leading >= 32 {
		leading = 31
	}

	if a.leading != 0xff && leading >= a.leading && trailing >= a.trailing {
		a.b.writeBit(zero)
		a.b.writeBits(vDelta>>a.trailing, 64-int(a.leading)-int(a.trailing))
	} else {
		a.leading, a.trailing = leading, trailing

		a.b.writeBit(one)
		a.b.writeBits(uint64(leading), 5)

		// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
		// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
		// So instead we write out a 0 and adjust it back to 64 on unpacking.
		sigbits := 64 - leading - trailing
		a.b.writeBits(uint64(sigbits), 6)
		a.b.writeBits(vDelta>>trailing, int(sigbits))
	}
}

type xorIterator struct {
	br       *bstream
	numTotal uint16
	numRead  uint16

	t   int64
	val float64

	leading  uint8
	trailing uint8

	tDelta uint64
	err    error
}

func (it *xorIterator) At() (int64, float64) {
	return it.t, it.val
}

func (it *xorIterator) Err() error {
	return it.err
}

func (it *xorIterator) Next() bool {
	if it.err != nil || len(it.br.stream) == 0 || (len(it.br.stream) == 1 && it.br.count == 0) {
		return false
	}

	if it.numRead == 0 {
		t, err := it.br.readBits(56) // unlike Gorilla we read a 56bit cropped int (time in year 2000+ has 48bit)
		//t, err := binary.ReadVarint(it.br)
		if err != nil {
			it.err = err
			return false
		}
		t = t & ((0x80 << 40) - 1)
		v, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}
		it.t = int64(t)
		it.val = math.Float64frombits(v)

		it.numRead++
		return true
	}

	// check if this a starting from scratch, signature is 111110xx
	isRestart := (it.br.PeekByte() & 0xfc) == 0xf8

	if it.numRead == 1 && !isRestart {
		tDelta, err := it.br.readBits(32)
		if err != nil {
			it.err = err
			return false
		}
		it.tDelta = tDelta
		it.t = it.t + int64(it.tDelta)

		rv := it.readValue()
		it.br.padToByte()

		return rv
	}

	var d byte
	// read delta-of-delta
	for i := 0; i < 5; i++ {
		d <<= 1
		bit, err := it.br.readBit()
		if err != nil {
			it.err = err
			return false
		}
		if bit == zero {
			break
		}
		d |= 1
	}
	var sz uint8
	var dod int64
	switch d {
	case 0x00:
		// dod == 0
	case 0x02:
		sz = 14
	case 0x06:
		sz = 17
	case 0x0e:
		sz = 20
	case 0x1e:
		bits, err := it.br.readBits(32)
		if err != nil {
			it.err = err
			return false
		}

		dod = int64(int32(bits))
	case 0x1f:
		// added this case to allow append of a new Gorilla series on an existing chunk (restart from t0)

		t, err := it.br.readBits(51)
		//t, err := binary.ReadVarint(it.br)
		if err != nil {
			it.err = err
			return false
		}
		//t = t & ((0x80 << 40) - 1)
		v, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}
		it.t = int64(t)
		it.val = math.Float64frombits(v)

		it.numRead = 1
		return true
	}

	if sz != 0 {
		bits, err := it.br.readBits(int(sz))
		if err != nil {
			it.err = err
			return false
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		dod = int64(bits)
	}

	it.tDelta = uint64(int64(it.tDelta) + dod)
	it.t = it.t + int64(it.tDelta)

	rv := it.readValue()
	it.br.padToByte()

	return rv
}

func (it *xorIterator) readValue() bool {
	bit, err := it.br.readBit()
	if err != nil {
		it.err = err
		return false
	}

	if bit == zero {
		// it.val = it.val
	} else {
		bit, err := it.br.readBit()
		if err != nil {
			it.err = err
			return false
		}
		if bit == zero {
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := it.br.readBits(5)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = uint8(bits)

			bits, err = it.br.readBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 64 - it.leading - mbits
		}

		mbits := int(64 - it.leading - it.trailing)
		bits, err := it.br.readBits(mbits)
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.val)
		vbits ^= (bits << it.trailing)
		it.val = math.Float64frombits(vbits)
	}

	it.numRead++
	return true
}
