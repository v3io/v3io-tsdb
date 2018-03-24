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
	"github.com/v3io/v3io-go-http"
)

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

func NewItemsCursor(container *v3io.Container, input *v3io.GetItemsInput, response *v3io.Response) *V3ioItemsCursor {
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

func (ic *V3ioItemsCursor) GetField(name string) interface{} {
	return (*ic.currentItem)[name]
}

// convert v3io blob to Int array
func AsInt64Array(val []byte) []uint64 {
	var array []uint64
	bytes := val
	for i := 16; i+8 <= len(bytes); i += 8 {
		val := binary.LittleEndian.Uint64(bytes[i : i+8])
		array = append(array, val)
	}
	return array
}
