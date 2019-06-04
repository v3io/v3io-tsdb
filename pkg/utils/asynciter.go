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
	"fmt"
	"net/http"
	"strings"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go/pkg/dataplane"
	"github.com/v3io/v3io-go/pkg/errors"
	"github.com/v3io/v3io-tsdb/pkg/config"
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
	container    v3io.Container
	logger       logger.Logger

	responseChan  chan *v3io.Response
	workers       int
	totalSegments int
	lastShards    int
	Cnt           int
}

func NewAsyncItemsCursor(container v3io.Container, input *v3io.GetItemsInput, workers int, shardingKeys []string, logger logger.Logger) (*AsyncItemsCursor, error) {

	// TODO: use workers from Context.numWorkers (if no ShardingKey)
	if workers == 0 || input.ShardingKey != "" {
		workers = 1
	}

	newAsyncItemsCursor := &AsyncItemsCursor{
		container:    container,
		input:        input,
		responseChan: make(chan *v3io.Response, 1000),
		workers:      workers,
		logger:       logger.GetChild("AsyncItemsCursor"),
	}

	if len(shardingKeys) > 0 {
		newAsyncItemsCursor.workers = len(shardingKeys)

		for i := 0; i < newAsyncItemsCursor.workers; i++ {
			input := v3io.GetItemsInput{
				Path:           input.Path,
				AttributeNames: input.AttributeNames,
				Filter:         input.Filter,
				ShardingKey:    shardingKeys[i],
			}
			_, err := container.GetItems(&input, &input, newAsyncItemsCursor.responseChan)

			if err != nil {
				return nil, err
			}
		}

		return newAsyncItemsCursor, nil
	}

	for i := 0; i < newAsyncItemsCursor.workers; i++ {
		newAsyncItemsCursor.totalSegments = workers
		input := v3io.GetItemsInput{
			Path:           input.Path,
			AttributeNames: input.AttributeNames,
			Filter:         input.Filter,
			TotalSegments:  newAsyncItemsCursor.totalSegments,
			Segment:        i,
		}
		_, err := container.GetItems(&input, &input, newAsyncItemsCursor.responseChan)

		if err != nil {
			// TODO: proper exit, release requests which passed
			return nil, err
		}
	}

	return newAsyncItemsCursor, nil
}

// error returns the last error
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
	for {
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

		err := ic.processResponse()
		if err != nil {
			return nil, err
		}
	}
}

func (ic *AsyncItemsCursor) processResponse() error {
	// Read response from channel
	resp := <-ic.responseChan
	defer resp.Release()

	// Ignore 404s
	if e, hasErrorCode := resp.Error.(v3ioerrors.ErrorWithStatusCode); hasErrorCode && e.StatusCode() == http.StatusNotFound {
		ic.logger.Debug("Got 404 - error: %v, request: %v", resp.Error, resp.Request().Input)
		ic.lastShards++
		return nil
	}
	if resp.Error != nil {
		ic.logger.Warn("error reading from response channel: %v, error: %v, request: %v", resp, resp.Error, resp.Request().Input)
		return errors.Wrap(resp.Error, "Failed to get next items")
	}

	getItemsResp := resp.Output.(*v3io.GetItemsOutput)

	// set the cursor items and reset the item index
	ic.items = getItemsResp.Items
	ic.itemIndex = 0

	conf, err := config.GetOrDefaultConfig()
	if err != nil {
		return err
	}

	// until IGZ-2.0 there is a bug in Nginx regarding range-scan, the following code is a mitigation for it.
	if *conf.DisableNginxMitigation {
		ic.sendNextGetItemsOld(resp)
	} else {
		ic.sendNextGetItemsNew(resp)
	}

	return nil
}

func (ic *AsyncItemsCursor) sendNextGetItemsOld(resp *v3io.Response) error {
	getItemsResp := resp.Output.(*v3io.GetItemsOutput)
	if !getItemsResp.Last {

		// if not last, make a new request to that shard
		input := resp.Context.(*v3io.GetItemsInput)

		// set next marker
		input.Marker = getItemsResp.NextMarker

		_, err := ic.container.GetItems(input, input, ic.responseChan)
		if err != nil {
			return errors.Wrap(err, "Failed to request next items")
		}

	} else {
		// Mark one more shard as completed
		ic.lastShards++
	}

	return nil
}

func (ic *AsyncItemsCursor) sendNextGetItemsNew(resp *v3io.Response) error {
	getItemsResp := resp.Output.(*v3io.GetItemsOutput)
	if len(getItemsResp.Items) > 0 {

		// if not last, make a new request to that shard
		input := resp.Context.(*v3io.GetItemsInput)

		if getItemsResp.NextMarker == "" || getItemsResp.NextMarker == input.Marker {
			lastItemObjectName, err := ic.items[len(ic.items)-1].GetFieldString(config.ObjectNameAttrName)

			splittdObjectName := strings.Split(lastItemObjectName, ".")

			// If we have the __name attribute and the query was a range scan query and the data is in range-scan format
			if len(splittdObjectName) == 2 && input.ShardingKey != "" && err == nil {
				lastSortingKey := splittdObjectName[1]

				ic.logger.Debug("getting next items after calculating next marker for %v%v is %v for the object=%v", input.Path, input.ShardingKey, lastSortingKey, lastItemObjectName)
				input.SortKeyRangeStart = lastSortingKey + "0"
				input.Marker = ""
			} else {
				// In case it is names query
				if getItemsResp.Last {
					ic.lastShards++
					return nil
				} else {
					input.Marker = getItemsResp.NextMarker
				}
			}
		} else {
			// set next marker
			input.Marker = getItemsResp.NextMarker
			ic.logger.Debug("getting next items for %v%v with given next marker %v", input.Path, input.ShardingKey, input.Marker)
		}

		_, err := ic.container.GetItems(input, input, ic.responseChan)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to request next items for input=%v", input))
		}

	} else {
		// Mark one more shard as completed
		ic.lastShards++
	}

	return nil
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
