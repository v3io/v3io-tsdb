package v3io

import (
	"errors"
)

var ErrInvalidTypeConversion = errors.New("Invalid type conversion")

type SyncItemsCursor struct {
	currentItem     Item
	currentError    error
	currentResponse *Response
	nextMarker      string
	moreItemsExist  bool
	itemIndex       int
	items           []Item
	input           *GetItemsInput
	container       *SyncContainer
}

func newSyncItemsCursor(container *SyncContainer, input *GetItemsInput) (*SyncItemsCursor, error) {
	newSyncItemsCursor := &SyncItemsCursor{
		container: container,
		input:     input,
	}

	response, err := container.GetItems(input)
	if err != nil {
		return nil, err
	}

	newSyncItemsCursor.setResponse(response)

	return newSyncItemsCursor, nil
}

// Err returns the last error
func (ic *SyncItemsCursor) Err() error {
	return ic.currentError
}

// Release releases a cursor and its underlying resources
func (ic *SyncItemsCursor) Release() {
	if ic.currentResponse != nil {
		ic.currentResponse.Release()
	}
}

// Next gets the next matching item. this may potentially block as this lazy loads items from the collection
func (ic *SyncItemsCursor) Next() bool {
	item, err := ic.NextItem()

	if item == nil || err != nil {
		return false
	}

	return true
}

// NextItem gets the next matching item. this may potentially block as this lazy loads items from the collection
func (ic *SyncItemsCursor) NextItem() (Item, error) {

	// are there any more items left in the previous response we received?
	if ic.itemIndex < len(ic.items) {
		ic.currentItem = ic.items[ic.itemIndex]
		ic.currentError = nil

		// next time we'll give next item
		ic.itemIndex++

		return ic.currentItem, nil
	}

	// are there any more items up stream?
	if !ic.moreItemsExist {
		ic.currentError = nil
		return nil, nil
	}

	// get the previous request input and modify it with the marker
	ic.input.Marker = ic.nextMarker

	// invoke get items
	newResponse, err := ic.container.GetItems(ic.input)
	if err != nil {
		return nil, err
	}

	// release the previous response
	ic.currentResponse.Release()

	// set the new response - read all the sub information from it
	ic.setResponse(newResponse)

	// and recurse into next now that we repopulated response
	return ic.NextItem()
}

// gets all items
func (ic *SyncItemsCursor) All() ([]Item, error) {
	var items []Item

	for ic.Next() {
		items = append(items, ic.GetItem())
	}

	if ic.Err() != nil {
		return nil, ic.Err()
	}

	return items, nil
}

func (ic *SyncItemsCursor) GetField(name string) interface{} {
	return ic.currentItem[name]
}

func (ic *SyncItemsCursor) GetFieldInt(name string) (int, error) {
	return ic.currentItem.GetFieldInt(name)
}

func (ic *SyncItemsCursor) GetFieldString(name string) (string, error) {
	return ic.currentItem.GetFieldString(name)
}

func (ic *SyncItemsCursor) GetFields() map[string]interface{} {
	return ic.currentItem
}

func (ic *SyncItemsCursor) GetItem() Item {
	return ic.currentItem
}

func (ic *SyncItemsCursor) setResponse(response *Response) {
	ic.currentResponse = response

	getItemsOutput := response.Output.(*GetItemsOutput)

	ic.moreItemsExist = !getItemsOutput.Last
	ic.nextMarker = getItemsOutput.NextMarker
	ic.items = getItemsOutput.Items
	ic.itemIndex = 0
}
