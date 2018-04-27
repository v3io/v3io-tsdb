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

	"math/rand"
)

const basetime = 1524690488000

type sample struct {
	t int64
	v float64
}

// [132 180 199 187 191 88 63 240 - 0 0 0 0 0 0 154 8 - 194 95 255 108 7 126 113 172 - 46 18 195 104 59 202 237 129 - 119 243 146]

func TestXor(tst *testing.T) {
	//ts1 := []int64{2000, 3050, 4100, 4950, 7000, 8200, 9300}
	//arr1 := []float64{1, 2, 3, 4, 5, 6, 7}
	samples := GenSamples(9, 1)
	byteArray := []byte{}

	ch := NewXORChunk()
	appender, err := ch.Appender()
	if err != nil {
		tst.Fatal(err)
	}

	for i, s := range samples {
		fmt.Println("t,v: ", s.t, s.v)
		appender.Append(s.t, s.v)
		b := ch.Bytes()
		fmt.Println(b, len(b))
		byteArray = append(byteArray, b...)
		ch.Clear()
		if i == 4 {
			fmt.Println("restarted appender")
			ch = NewXORChunk()
			appender, err = ch.Appender()
			if err != nil {
				tst.Fatal(err)
			}

		}
	}

	fmt.Println("byteArray", byteArray, len(byteArray))

	ch2, err := FromData(EncXOR, byteArray, 0)
	if err != nil {
		tst.Fatal(err)
	}

	iter := ch2.Iterator()
	i := 0
	for iter.Next() {

		if iter.Err() != nil {
			tst.Fatal(iter.Err())
		}

		t, v := iter.At()
		isMatch := t == samples[i].t && v == samples[i].v
		fmt.Println("t, v, match: ", t, v, isMatch)
		if !isMatch {
			tst.Fatalf("iterator t or v doesnt match appended", i, len(samples))
		}
		i++
	}
	fmt.Println()

	if i != len(samples) {
		tst.Fatalf("number of iterator samples (%d) != num of appended (%d)", i, len(samples))
	}

}

func TestBstream(t *testing.T) {
	src := &bstream{count: 8, stream: []byte{0x55, 0x44, 0x33}}

	bs := newBWriter(8)
	byt, _ := src.readByte()
	bs.writeByte(byt)
	fmt.Println(bs.count, bs.stream)
	for i := 1; i < 18; i++ {
		bit, _ := src.readBit()
		fmt.Println(bs.count, bs.stream, bit)
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

func GenSamples(num, interval int) []sample {
	samples := []sample{}
	curTime := int64(basetime)

	for i := 0; i <= num; i++ {
		curTime += int64(interval * 1000)
		t := curTime + int64(rand.Intn(100)) - 50
		v := rand.Float64() * 1000
		//fmt.Printf("t-%d,v%.2f ", t, v)
		samples = append(samples, sample{t: t, v: v})
	}

	return samples
}
