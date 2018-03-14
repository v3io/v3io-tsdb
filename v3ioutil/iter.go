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

package v3ioutil

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/chunkenc"
	"math"
	"strings"
)

func asInt64Array(val []byte) []uint64 {
	var array []uint64
	bytes := val
	for i := 16; i+8 <= len(bytes); i += 8 {
		val := binary.LittleEndian.Uint64(bytes[i : i+8])
		array = append(array, val)
	}
	return array
}

type V3ioItemsCursor struct {
	currentItem    *map[string]interface{}
	lastError      error
	nextMarker     string
	moreItemsExist bool
	itemIndex      int
	items          []v3io.Item
	response       *v3io.Response
	input          *v3io.GetItemsInput
	container      *v3io.Container
}

func newItemsCursor(container *v3io.Container, input *v3io.GetItemsInput, response *v3io.Response) *V3ioItemsCursor {
	newItemsCursor := &V3ioItemsCursor{
		container: container,
		input:     input,
	}

	newItemsCursor.setResponse(response)

	return newItemsCursor
}

func (ic *V3ioItemsCursor) Err() error {
	return ic.lastError
}

// release a cursor and its underlying resources
func (ic *V3ioItemsCursor) Release() {
	ic.response.Release()
}

// get the next matching item. this may potentially block as this lazy loads items from the collection
func (ic *V3ioItemsCursor) Next() bool {

	// are there any more items left in the previous response we received?
	if ic.itemIndex < len(ic.items) {
		item := map[string]interface{}(ic.items[ic.itemIndex])
		ic.currentItem = &item

		// next time we'll give next item
		ic.itemIndex++
		ic.lastError = nil

		return true
	}

	// are there any more items up stream?
	if !ic.moreItemsExist {
		ic.currentItem = nil
		return false
	}

	// get the previous request input and modify it with the marker
	ic.input.Marker = ic.nextMarker

	// invoke get items
	newResponse, err := ic.container.Sync.GetItems(ic.input)
	if err != nil {
		ic.lastError = errors.Wrap(err, "Failed to request next items")
		ic.currentItem = nil
		return false
	}

	// release the previous response
	ic.response.Release()

	// set the new response - read all the sub information from it
	ic.setResponse(newResponse)

	// and recurse into next now that we repopulated response
	return ic.Next()
}

func (ic *V3ioItemsCursor) setResponse(response *v3io.Response) {
	ic.response = response

	getItemsOutput := response.Output.(*v3io.GetItemsOutput)

	ic.moreItemsExist = !getItemsOutput.Last
	ic.nextMarker = getItemsOutput.NextMarker
	ic.items = getItemsOutput.Items
	ic.itemIndex = 0
}

func (ic *V3ioItemsCursor) GetLables() labels.Labels {
	name := (*ic.currentItem)["__name"].(string)
	split := strings.SplitN(name, ".", 2)
	lset := labels.Labels{labels.Label{Name: "__name__", Value: split[0]}}

	if len(split) < 2 {
		return lset
	}

	lbl := strings.Split(split[1], KEY_SEPERATOR)
	for i := 0; i+2 <= len(lbl); i += 2 {
		lset = append(lset, labels.Label{Name: lbl[i], Value: lbl[i+1]})
	}

	return lset
}

func (ic *V3ioItemsCursor) GetField(name string) interface{} {
	return (*ic.currentItem)[name]
}

func (ic *V3ioItemsCursor) GetSeriesIter(mint, maxt int64) *v3ioSeriesIterator {
	newIterator := v3ioSeriesIterator{mint: mint, maxt: maxt}
	values, ok := (*ic.currentItem)["_values"]
	if ok && len(values.([]byte)) >= 24 {
		bytes := values.([]byte)
		chunk, _ := chunkenc.FromBuffer(binary.LittleEndian.Uint64(bytes[16:24]), bytes[24:])
		// TODO: err handle

		//c, _ := chunkenc.FromData(chunkenc.EncXOR, bytes[24:], count)
		newIterator.chunk = chunk
		newIterator.iter = chunk.Iterator()

	}
	return &newIterator
}

type v3ioSeriesIterator struct {
	mint, maxt int64 // TBD per block
	err        error

	chunk chunkenc.Chunk //TODO: need to be array
	iter  chunkenc.Iterator
}

func (it *v3ioSeriesIterator) Seek(t int64) bool {
	if t > it.maxt {
		return false
	}

	// Seek to the first valid value after t.
	if t < it.mint {
		t = it.mint
	}

	// TODO: check if T < chunk start + max hours in chunk
	//for ; it.chunks[it.i].MaxTime < t; it.i++ {
	//	if it.i == len(it.chunks)-1 {
	//		return false
	//	}
	//}

	// TODO multiple chunks

	for it.iter.Next() {
		t0, _ := it.At()
		if t0 >= t {
			return true
		}
	}
	return false
}

func (it *v3ioSeriesIterator) Next() bool {
	if it.iter.Next() {
		t, _ := it.iter.At()

		if t < it.mint {
			if !it.Seek(it.mint) {
				return false
			}
			t, _ = it.At()

			return t <= it.maxt
		}
		if t > it.maxt {
			return false
		}
		return true
	}

	return false
}

func (it *v3ioSeriesIterator) At() (t int64, v float64) { return it.iter.At() }

func (it *v3ioSeriesIterator) Err() error { return it.iter.Err() }

func uintToTV(data uint64, curT int64, curV float64) (int64, float64) {
	v := float64(math.Float32frombits(uint32(data)))
	t := int64(data >> 32)
	return curT + t, curV + v
}
