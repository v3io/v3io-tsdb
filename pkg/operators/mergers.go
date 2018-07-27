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

type IterSortMerger struct {
	iters      []ChildIterator
	done       []bool
	currKey    int64
	currValids []bool
	currIters  []ChildIterator
	err        error
}

func NewIterSortMerger() *IterSortMerger {
	newMerger := IterSortMerger{}
	newMerger.iters = make([]ChildIterator, 0)
	newMerger.done = make([]bool, 0)
	newMerger.currValids = make([]bool, 0)
	return &newMerger
}

func (im *IterSortMerger) AppendIter(iter ChildIterator) {
	im.iters = append(im.iters, iter)
	im.done = append(im.done, false) //!iter.Next())
	im.currValids = append(im.currValids, true)
}

func (im *IterSortMerger) Next() bool {

	completed := true
	keyIsSet := false
	for i, iter := range im.iters {
		if im.currValids[i] {
			im.done[i] = !iter.Next()
			if iter.Err() != nil {
				im.err = iter.Err()
				return false
			}
			completed = completed && im.done[i]
		}
		if !im.done[i] {
			key := iter.GetKey()
			if !keyIsSet {
				im.currKey = key
				keyIsSet = true
			} else if key < im.currKey {
				im.currKey = key
			}
		}
	}

	if completed {
		return false
	}

	im.currIters = im.currIters[:0]
	for i, iter := range im.iters {
		im.currValids[i] = false
		if !im.done[i] {
			if iter.GetKey() == im.currKey {
				im.currValids[i] = true
				im.currIters = append(im.currIters, iter)
			}
		}
	}

	return true
}

// return the current key and list of iterators containing it
func (im *IterSortMerger) At() (int64, []bool, []ChildIterator) {
	return im.currKey, im.currValids, im.currIters
}

func (im *IterSortMerger) Err() error {
	return im.err
}

func (im *IterSortMerger) AllDone() bool {
	completed := true
	for _, done := range im.done {
		completed = completed && done
	}
	return completed
}

// Interface requiered by the merge sorted iterator
type ChildIterator interface {
	Next() bool
	GetKey() int64
	Err() error
}
