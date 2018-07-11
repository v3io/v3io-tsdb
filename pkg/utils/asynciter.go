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

import (
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go-http"
)

type ItemsCursor interface {
	Err() error
	Next() bool
	GetField(name string) interface{}
	GetFields() map[string]interface{}
}

type AsyncItemsCursor struct {
	currentItem  v3io.Item
	currentError error
	itemIndex    int
	items        []v3io.Item
	input        *v3io.GetItemsInput
	container    *v3io.Container
	logger       logger.Logger

	responseChan  chan *v3io.Response
	workers       int
	totalSegments int
	lastShards    int
	Cnt           int
}

func NewAsyncItemsCursor(container *v3io.Container, input *v3io.GetItemsInput, workers int) (*AsyncItemsCursor, error) {

	// TODO: use workers from Context.numWorkers (if no ShardingKey)
	if workers == 0 || input.ShardingKey != "" {
		workers = 1
	}

	newAsyncItemsCursor := &AsyncItemsCursor{
		container:    container,
		input:        input,
		responseChan: make(chan *v3io.Response, 1000),
		workers:      workers,
	}

	if input.ShardingKey != "" {
		newAsyncItemsCursor.workers = 1
		input := v3io.GetItemsInput{
			Path: input.Path, AttributeNames: input.AttributeNames, Filter: input.Filter,
			ShardingKey: input.ShardingKey}
		_, err := container.GetItems(&input, 0, newAsyncItemsCursor.responseChan)

		if err != nil {
			return nil, err
		}

		return newAsyncItemsCursor, nil
	}

	for i := 0; i < newAsyncItemsCursor.workers; i++ {
		newAsyncItemsCursor.totalSegments = workers
		input := v3io.GetItemsInput{
			Path: input.Path, AttributeNames: input.AttributeNames, Filter: input.Filter,
			TotalSegments: newAsyncItemsCursor.totalSegments, Segment: i}
		_, err := container.GetItems(&input, i, newAsyncItemsCursor.responseChan)

		if err != nil {
			// TODO: proper exit, release requests which passed
			return nil, err
		}
	}

	return newAsyncItemsCursor, nil
}

// Err returns the last error
func (ic *AsyncItemsCursor) Err() error {
	return ic.currentError
}

// Release releases a cursor and its underlying resources
func (ic *AsyncItemsCursor) Release() {
	// TODO:
}

// Next gets the next matching item. this may potentially block as this lazy loads items from the collection
func (ic *AsyncItemsCursor) Next() bool {
	item, err := ic.NextItem()

	if item == nil || err != nil {
		ic.currentError = err
		return false
	}

	return true
}

// NextItem gets the next matching item. this may potentially block as this lazy loads items from the collection
func (ic *AsyncItemsCursor) NextItem() (v3io.Item, error) {

	// are there any more items left in the previous response we received?
	if ic.itemIndex < len(ic.items) {
		ic.currentItem = ic.items[ic.itemIndex]
		ic.currentError = nil

		// next time we'll give next item
		ic.itemIndex++
		ic.Cnt++

		return ic.currentItem, nil
	}

	// are there any more items up stream? did all the shards complete ?
	if ic.lastShards == ic.workers {
		ic.currentError = nil
		return nil, nil
	}

	// Read response from channel
	resp := <-ic.responseChan
	if resp.Error != nil {
		return nil, errors.Wrap(resp.Error, "Failed to get next items")
	}

	getItemsResp := resp.Output.(*v3io.GetItemsOutput)
	shard := resp.Context.(int)
	//fmt.Println("got resp:",shard, len(getItemsResp.Items), getItemsResp.Last)
	resp.Release()

	// set the cursor items and reset the item index
	ic.items = getItemsResp.Items
	ic.itemIndex = 0

	if !getItemsResp.Last {
		// if not last, make a new request to that shard

		input := v3io.GetItemsInput{
			Path: ic.input.Path, AttributeNames: ic.input.AttributeNames, Filter: ic.input.Filter,
			TotalSegments: ic.totalSegments, Segment: shard, Marker: getItemsResp.NextMarker}
		_, err := ic.container.GetItems(&input, shard, ic.responseChan)

		if err != nil {
			return nil, errors.Wrap(resp.Error, "Failed to request next items")
		}

	} else {
		//fmt.Println("last",shard,len(getItemsResp.Items))
		// Mark one more shard as completed
		ic.lastShards++
	}

	// and recurse into next now that we repopulated response
	return ic.NextItem()
}

// gets all items
func (ic *AsyncItemsCursor) All() ([]v3io.Item, error) {
	var items []v3io.Item

	for ic.Next() {
		items = append(items, ic.GetItem())
	}

	if ic.Err() != nil {
		return nil, ic.Err()
	}

	return items, nil
}

func (ic *AsyncItemsCursor) GetField(name string) interface{} {
	return ic.currentItem[name]
}

func (ic *AsyncItemsCursor) GetFieldInt(name string) (int, error) {
	return ic.currentItem.GetFieldInt(name)
}

func (ic *AsyncItemsCursor) GetFieldString(name string) (string, error) {
	return ic.currentItem.GetFieldString(name)
}

func (ic *AsyncItemsCursor) GetFields() map[string]interface{} {
	return ic.currentItem
}

func (ic *AsyncItemsCursor) GetItem() v3io.Item {
	return ic.currentItem
}
