package appender

import "sync"

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

	if eq.head >= eq.tail {
		return eq.head - eq.tail
	}

	return eq.head + (len(eq.data) * ListSize) - eq.tail
}

// Push a value to the queue
func (eq *ElasticQueue) Push(val *MetricState) bool {
	eq.mtx.Lock()
	defer eq.mtx.Unlock()

	headBlock, headOffset := eq.head/ListSize, eq.head%ListSize
	tailBlock := eq.tail / ListSize
	wasEmpty := eq.head == eq.tail

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
	return wasEmpty
}

func (eq *ElasticQueue) Pop() *MetricState {
	eq.mtx.Lock()
	defer eq.mtx.Unlock()

	return eq.pop()
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
