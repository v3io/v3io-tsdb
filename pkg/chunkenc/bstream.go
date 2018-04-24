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
	"io"
)

// bstream is a stream of bits.
type bstream struct {
	stream     []byte // the data stream
	count      uint8  // how many bits are valid in current byte
	rptr, wptr uint16
	isStream   bool
}

func newBReader(b []byte) *bstream {
	return &bstream{stream: b, count: 8}
}

func newBWriter(size int) *bstream {
	bstr := bstream{count: 0}
	if size == 0 {
		bstr.stream = []byte{}
	} else {
		bstr.isStream = true
		bstr.stream = make([]byte, size, size)
	}
	return &bstr
}

func (b *bstream) clone() *bstream {
	d := make([]byte, len(b.stream))
	copy(d, b.stream)
	return &bstream{stream: d, count: b.count, wptr: b.wptr}
}

func (b *bstream) bytes() []byte {
	//fmt.Println("bytes: rptr, wptr, bits =", b.wrapPtr(b.rptr), b.wrapPtr(b.wptr), b.count, len(b.stream), cap(b.stream), b.stream)

	if !b.isStream {
		return b.stream
	}

	wptr := b.getLen()
	if b.wrapPtr(b.rptr) <= b.wrapPtr(wptr) {
		return b.stream[b.wrapPtr(b.rptr):b.wrapPtr(wptr)]
	}

	return append(b.stream[b.wrapPtr(b.rptr):len(b.stream)], b.stream[0:b.wrapPtr(wptr)]...)
}

func (b *bstream) getOffset() uint16 {
	return b.rptr
}

func (b *bstream) getLen() uint16 {
	if b.count != 8 {
		return b.wptr + 1
	}
	return b.wptr
}

type bit bool

const (
	zero bit = false
	one  bit = true
)

func (b *bstream) wrapPtr(ptr uint16) uint16 {
	if !b.isStream {
		return ptr
	}

	return ptr & uint16(len(b.stream)-1)
}

func (b *bstream) writeBit(bit bit) {
	if b.count == 0 {
		b.appendZero()
	}

	if bit {
		b.stream[b.wrapPtr(b.wptr)] |= 1 << (b.count - 1)
	}

	b.count--
}

func (b *bstream) padToByte() {
	if b.count != 8 {
		b.count = 0
	}
}

func (b *bstream) writeByte(byt byte) {
	if b.count == 0 {
		b.appendZero()
	}

	// fill up b.b with b.count bits from byt
	b.stream[b.wrapPtr(b.wptr)] |= byt >> (8 - b.count)
	b.wrapAppend(byt << b.count)
}

func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= (64 - uint(nbits))
	for nbits >= 8 {
		byt := byte(u >> 56)
		b.writeByte(byt)
		u <<= 8
		nbits -= 8
	}

	for nbits > 0 {
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

func (b *bstream) wrapAppend(byt byte) {

	b.wptr = b.wptr + 1
	if b.isStream {
		b.stream[b.wrapPtr(b.wptr)] = byt
	} else {
		b.stream = append(b.stream, byt)
	}

}

func (b *bstream) appendZero() {

	if b.wptr != 0 {
		b.wptr = b.wptr + 1
	}
	if b.isStream {
		b.stream[b.wrapPtr(b.wptr)] = 0
	} else {
		b.stream = append(b.stream, 0)
	}
	b.count = 8
}

func (b *bstream) readBit() (bit, error) {
	if len(b.stream) == 0 {
		return false, io.EOF
	}

	if b.count == 0 {
		b.stream = b.stream[1:]

		if len(b.stream) == 0 {
			return false, io.EOF
		}
		b.count = 8
	}

	d := (b.stream[0] << (8 - b.count)) & 0x80
	b.count--
	return d != 0, nil
}

func (b *bstream) ReadByte() (byte, error) {
	return b.readByte()
}

func (b *bstream) readByte() (byte, error) {
	if len(b.stream) == 0 {
		return 0, io.EOF
	}

	if b.count == 0 {
		b.stream = b.stream[1:]

		if len(b.stream) == 0 {
			return 0, io.EOF
		}
		return b.stream[0], nil
	}

	if b.count == 8 {
		b.count = 0
		return b.stream[0], nil
	}

	byt := b.stream[0] << (8 - b.count)
	b.stream = b.stream[1:]

	if len(b.stream) == 0 {
		return 0, io.EOF
	}

	// We just advanced the stream and can assume the shift to be 0.
	byt |= b.stream[0] >> b.count

	return byt, nil
}

func (b *bstream) readBits(nbits int) (uint64, error) {
	var u uint64

	for nbits >= 8 {
		byt, err := b.readByte()
		if err != nil {
			return 0, err
		}

		u = (u << 8) | uint64(byt)
		nbits -= 8
	}

	if nbits == 0 {
		return u, nil
	}

	if nbits > int(b.count) {
		u = (u << uint(b.count)) | uint64((b.stream[0]<<(8-b.count))>>(8-b.count))
		nbits -= int(b.count)
		b.stream = b.stream[1:]

		if len(b.stream) == 0 {
			return 0, io.EOF
		}
		b.count = 8
	}

	u = (u << uint(nbits)) | uint64((b.stream[0]<<(8-b.count))>>(8-uint(nbits)))
	b.count -= uint8(nbits)
	return u, nil
}
