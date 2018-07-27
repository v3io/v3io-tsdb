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

package operators

import (
	"fmt"
	"math/rand"
	"sort"
)

/*  Example usage
func main() {
	merger := NewIterSortMerger()
	merger.AppendIter(NewIntIter(5,3, 10))
	merger.AppendIter(NewIntIter(7,0, 9))
	merger.AppendIter(NewIntIter(10,0, 15))
	fmt.Println(merger.iters)

	for merger.Next() {
		//print next key and the indexes/values of the iterators containing it
		key, valids, list := merger.At()
		fmt.Println(key, valids, list)
	}
}
*/

// Example Iterator (int list)
type IntIter struct {
	list  []int64
	index int
	err   error
}

func NewIntIter(size int, base, max int64) *IntIter {
	list := GenUniqueSortList(size, base, max)
	return &IntIter{list: list, index: -1}
}

func (i *IntIter) String() string {
	return fmt.Sprintf("%v", i.list)
}

func (i *IntIter) Next() bool {
	if i.index < len(i.list)-1 {
		i.index++
		return true
	}
	return false
}

func (i *IntIter) GetKey() int64 {
	return i.list[i.index]
}

func (i *IntIter) GetValue() interface{} {
	return int(i.index)
}

func (i *IntIter) Err() error { return i.err }

// Unique int list generator
func GenUniqueSortList(size int, base, max int64) []int64 {
	generated := map[int64]bool{}
	list := []int64{}

	for j := 0; j < size; j++ {
		for {
			i := rand.Int63n(max-base) + base
			_, found := generated[i]
			if !found {
				generated[i] = true
				list = append(list, i)
				break
			}
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i] < list[j] })
	return list
}
