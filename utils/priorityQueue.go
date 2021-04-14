package utils

import (
	"container/heap"
)

type PriorityQueue struct {
	minHeap MinHeap
}

func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		make(MinHeap, 0),
	}
	heap.Init(&pq.minHeap)
	return pq
}

func (q *PriorityQueue) Pop() *Item {
	return heap.Pop(&q.minHeap).(*Item)
}

func (q *PriorityQueue) Peek() *Item {
	if q.minHeap.Len() == 0 {
		return nil
	}

	return q.minHeap[0]
}

func (q *PriorityQueue) Len() int {
	return q.minHeap.Len()
}

func (q *PriorityQueue) Push(op *Item) {
	heap.Push(&q.minHeap, op)
}

func (q *PriorityQueue) Remove(op *Item) {
	for i := 0; i < len(q.minHeap); i++ {
		if q.minHeap[i].TxnId == op.TxnId {
			q.minHeap[i], q.minHeap[len(q.minHeap)-1] = q.minHeap[len(q.minHeap)-1], q.minHeap[i]
			q.minHeap = q.minHeap[:len(q.minHeap)-1]
			break
		}
	}

	heap.Init(&q.minHeap)
}

type Item struct {
	Timestamp int
	TxnId     string
	ClientId  string
	index     int
}

type MinHeap []*Item

func (pq MinHeap) Len() int {
	return len(pq)
}

func (pq MinHeap) Less(i, j int) bool {
	requestI := pq[i]
	requestJ := pq[j]
	if requestI.Timestamp == requestJ.Timestamp {
		return requestI.TxnId < requestJ.TxnId
	}
	return requestI.Timestamp < requestJ.Timestamp
}

func (pq MinHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *MinHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *MinHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
