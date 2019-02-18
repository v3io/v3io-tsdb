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
	"math"
	"reflect"
	"testing"

	"github.com/v3io/frames"
)

func TestAppendValue(t *testing.T) {
	data := []int64{1, 2, 3}
	out, err := AppendValue(data, int64(4))
	if err != nil {
		t.Fatal(err)
	}

	expected := []int64{1, 2, 3, 4}
	if !reflect.DeepEqual(out, expected) {
		t.Fatalf("bad append %v != %v", out, expected)
	}

	_, err = AppendValue(data, "4")
	if err == nil {
		t.Fatal("no error on type mismatch")
	}

	_, err = AppendValue([]bool{true}, false)
	if err == nil {
		t.Fatal("no error on unknown mismatch")
	}
}

func TestNewColumn(t *testing.T) {
	i := int64(7)
	out, err := NewColumn(i, 3)
	if err != nil {
		t.Fatal(err)
	}

	expected := []int64{0, 0, 0}
	if !reflect.DeepEqual(out, expected) {
		t.Fatalf("bad new column %v != %v", out, expected)
	}

	_, err = NewColumn(uint(7), 2)
	if err == nil {
		t.Fatal("no error on unknown type")
	}
}

func TestAppendNil(t *testing.T) {
	size := 10
	data, err := NewColumn(1.0, size)
	if err != nil {
		t.Fatalf("can't create data - %s", err)
	}

	col, err := frames.NewSliceColumn("col1", data)
	if err != nil {
		t.Fatalf("can't create new slice column - %s", err)
	}

	if err := AppendNil(col); err != nil {
		t.Fatalf("can't append nil - %s", err)
	}

	if newSize := col.Len(); newSize != size+1 {
		t.Fatalf("bad size change - %d != %d", newSize, size+1)
	}

	val, _ := col.FloatAt(size)
	if !math.IsNaN(val) {
		t.Fatalf("AppendNil didn't add NaN to floats (got %v)", val)
	}
}

func TestRemoveColumn(t *testing.T) {
	colName := func(i int) string {
		return fmt.Sprintf("col-%d", i)
	}

	size := 7
	columns := make([]frames.Column, size)
	for i := 0; i < size; i++ {
		col, err := frames.NewSliceColumn(colName(i), []int64{})
		if err != nil {
			t.Fatalf("can't create column - %s", err)
		}
		columns[i] = col
	}

	name := colName(4)
	newCols := RemoveColumn(name, columns)
	if newSize := len(newCols); newSize != size-1 {
		t.Fatalf("size after remove %d (wanted %d)", newSize, size-1)
	}

	for _, col := range newCols {
		if col.Name() == name {
			t.Fatalf("column %q found after removal", name)
		}
	}

	newCols = RemoveColumn("no-such-column", columns)
	if len(newCols) != len(columns) {
		t.Fatal("no existing column removed")
	}
}
