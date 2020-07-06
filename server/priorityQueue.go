package server

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

func (q *PriorityQueue) Pop() *ReadAndPrepareGTS {
	return heap.Pop(&q.minHeap).(*ReadAndPrepareGTS)
}

func (q *PriorityQueue) Peek() *ReadAndPrepareGTS {
	if q.minHeap.Len() == 0 {
		return nil
	}

	return q.minHeap[0]
}

func (q *PriorityQueue) Len() int {
	return q.minHeap.Len()
}

func (q *PriorityQueue) Push(op *ReadAndPrepareGTS) {
	heap.Push(&q.minHeap, op)
}

func (q *PriorityQueue) Remove(op *ReadAndPrepareGTS) {
	for i := 0; i < len(q.minHeap); i++ {
		if q.minHeap[i].txnId == op.txnId {
			q.minHeap[i], q.minHeap[len(q.minHeap)-1] = q.minHeap[len(q.minHeap)-1], q.minHeap[i]
			q.minHeap = q.minHeap[:len(q.minHeap)-1]
			break
		}
	}

	heap.Init(&q.minHeap)
}

type MinHeap []*ReadAndPrepareGTS

func (pq MinHeap) Len() int {
	return len(pq)
}

func (pq MinHeap) Less(i, j int) bool {
	if pq[i].request.Timestamp == pq[j].request.Timestamp {
		if pq[i].request.ClientId == pq[i].request.ClientId {
			return pq[i].txnId < pq[i].txnId
		}
		return pq[i].request.ClientId < pq[i].request.ClientId
	}
	return pq[i].request.Timestamp < pq[j].request.Timestamp
}

func (pq MinHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *MinHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(*ReadAndPrepareGTS)
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
