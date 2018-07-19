package appender

import (
	"sync"
)

const ListSize = 256

type list [ListSize]*MetricState

type ElasticQueue struct {
	mtx        sync.RWMutex
	data       []*list
	head, tail int
}

// Elastic Queue, a fifo queue with dynamic resize
func NewElasticQueue() *ElasticQueue {
	newQueue := ElasticQueue{}
	newQueue.data = append(newQueue.data, &list{})
	return &newQueue
}

// Is the queue empty
func (eq *ElasticQueue) IsEmpty() bool {
	eq.mtx.Lock()
	defer eq.mtx.Unlock()

	return eq.head == eq.tail
}

// Number of elements in the queue
func (eq *ElasticQueue) Length() int {
	eq.mtx.Lock()
	defer eq.mtx.Unlock()

	return eq.length()
}

func (eq *ElasticQueue) length() int {
	if eq.head >= eq.tail {
		return eq.head - eq.tail
	}

	return eq.head + (len(eq.data) * ListSize) - eq.tail
}

func (eq *ElasticQueue) Push(val *MetricState) int {
	eq.mtx.Lock()
	defer eq.mtx.Unlock()

	return eq.push(val)
}

// Push a value to the queue
func (eq *ElasticQueue) push(val *MetricState) int {
	headBlock, headOffset := eq.head/ListSize, eq.head%ListSize
	tailBlock := eq.tail / ListSize
	//wasEmpty := eq.head == eq.tail

	if headBlock == tailBlock-1 && headOffset == ListSize-1 {
		eq.data = append(eq.data, &list{})
		copy(eq.data[tailBlock+1:], eq.data[tailBlock:])
		eq.data[tailBlock] = &list{}

		eq.tail += ListSize

	}

	if headBlock == len(eq.data)-1 && headOffset == ListSize-1 {
		if tailBlock == 0 {
			eq.data = append(eq.data, &list{})
		}
	}

	eq.head = (eq.head + 1) % (len(eq.data) * ListSize)
	eq.data[headBlock][headOffset] = val
	return eq.length()
}

func (eq *ElasticQueue) Pop() *MetricState {
	eq.mtx.Lock()
	defer eq.mtx.Unlock()

	return eq.pop()
}

func (eq *ElasticQueue) PopN(length int) []*MetricState {
	eq.mtx.Lock()
	defer eq.mtx.Unlock()
	var list []*MetricState

	for i := 0; i < length; i++ {
		metric := eq.pop()
		if metric != nil {
			list = append(list, metric)
		} else {
			break
		}
	}

	return list
}

// return the oldest value in the queue
func (eq *ElasticQueue) pop() *MetricState {
	if eq.head == eq.tail {
		return nil
	}

	tailBlock, tailOffset := eq.tail/ListSize, eq.tail%ListSize
	eq.tail = (eq.tail + 1) % (len(eq.data) * ListSize)

	return eq.data[tailBlock][tailOffset]
}

// Atomic rotate, push a value to the tail and pop one from the head
func (eq *ElasticQueue) Rotate(val *MetricState) (*MetricState, int) {
	eq.mtx.Lock()
	defer eq.mtx.Unlock()

	if eq.head == eq.tail {
		return val, 0
	}

	length := eq.push(val)
	return eq.pop(), length
}
