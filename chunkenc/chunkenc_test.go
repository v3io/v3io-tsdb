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
	"fmt"
	"testing"
	//"github.com/prometheus/tsdb/chunkenc"
)

const basetime = 1520346654002

// [132 180 199 187 191 88 63 240 - 0 0 0 0 0 0 154 8 - 194 95 255 108 7 126 113 172 - 46 18 195 104 59 202 237 129 - 119 243 146]

func TestXor(t *testing.T) {
	ts1 := []int64{2000, 3050, 4100, 4950, 7000, 8200, 9300}
	arr1 := []float64{1, 2, 3, 4, 5, 6, 7}
	barr := make([]byte, 128, 128)
	var cmeta uint64

	ch := NewXORChunk()
	appender, err := ch.Appender()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(arr1); i++ {
		fmt.Println("t,v: ", basetime+ts1[i], arr1[i])
		appender.Append(basetime+ts1[i], arr1[i])
		//b := ch.Bytes()
		meta, offset, b := ch.GetChunkBuffer()
		fmt.Println(getMetadata(meta), offset, b, len(b))
		cmeta = meta
		for i := 0; i < len(b); i++ {
			barr[i+offset] = b[i]
		}
		//fmt.Println()
		//ch.MoveOffset(uint16((offset+len(b)-1)/8) * 8)
		//if i == 4 { ch.MoveOffset(16)}
	}

	fmt.Println("barr", barr)

	ch2, err := FromBuffer(cmeta, barr)
	if err != nil {
		t.Fatal(err)
	}

	iter := ch2.Iterator()
	for iter.Next() {

		if iter.Err() != nil {
			t.Fatal(iter.Err())
		}

		t, v := iter.At()
		fmt.Printf("t=%d,v=%f ", t, v)
	}
	fmt.Println()

}

func TestBstream(t *testing.T) {
	src := &bstream{count: 8, stream: []byte{0x55, 0x44, 0x33}}

	bs := newBWriter(8)
	byt, _ := src.readByte()
	bs.writeByte(byt)
	fmt.Println(bs.count, bs.stream, bs.wptr)
	for i := 1; i < 18; i++ {
		bit, _ := src.readBit()
		fmt.Println(bs.count, bs.stream, bs.wptr, bit)
		bs.writeBit(bit)
	}

	fmt.Println("Reading:")
	bs2 := &bstream{count: 8, stream: bs.stream}
	fmt.Println(bs2.count, bs2.stream)
	for i := 1; i < 18; i++ {
		bit, _ := bs2.readBit()
		fmt.Println(bs2.count, bs2.stream, bit)
	}

}
