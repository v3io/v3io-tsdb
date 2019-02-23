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

package kv

import (
	"fmt"
	"github.com/v3io/frames"
	"github.com/v3io/frames/v3ioutils"
	"github.com/v3io/v3io-go-http"
)

func (b *Backend) inferSchema(request *frames.ExecRequest) error {

	container, table, err := b.newConnection(request.Session, request.Table, true)
	if err != nil {
		return err
	}

	keyField := "__name"
	if val, ok := request.Args["key"]; ok {
		keyField = val.GetSval()
	}
	maxrec := 50

	input := v3io.GetItemsInput{
		Path: table, Filter: "", AttributeNames: []string{"*"}}
	b.logger.DebugWith("GetItems for schema", "input", input)
	iter, err := v3ioutils.NewAsyncItemsCursor(
		container, &input, b.numWorkers, []string{}, b.logger, 0)
	if err != nil {
		return err
	}

	rowSet := []map[string]interface{}{}
	indicies := []string{}

	for rowNum := 0; rowNum < maxrec && iter.Next(); rowNum++ {
		row := iter.GetFields()
		rowSet = append(rowSet, row)
		index, ok := row["__name"]
		if !ok {
			return fmt.Errorf("key (__name) was not found in row")
		}
		indicies = append(indicies, index.(string))
	}

	if iter.Err() != nil {
		return iter.Err()
	}

	labels := map[string]interface{}{}
	frame, err := frames.NewFrameFromRows(rowSet, indicies, labels)
	if err != nil {
		return fmt.Errorf("Failed to create frame - %v", err)
	}

	nullSchema := v3ioutils.NewSchema(keyField)
	newSchema := v3ioutils.NewSchema(keyField)

	for _, name := range frame.Names() {
		col, err := frame.Column(name)
		if err != nil {
			return err
		}
		err = newSchema.AddColumn(name, col, true)
		if err != nil {
			return err
		}
	}

	return nullSchema.UpdateSchema(container, table, newSchema)
}
