package server

import "container/heap"

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

func (q *PriorityQueue) Pop() *ReadAndPrepareOp {
	return heap.Pop(&q.minHeap).(*ReadAndPrepareOp)
}

func (q *PriorityQueue) Peek() *ReadAndPrepareOp {
	if len(q.minHeap) == 0 {
		return nil
	}

	return q.minHeap[0]
}

func (q *PriorityQueue) Push(op *ReadAndPrepareOp) {
	heap.Push(&q.minHeap, op)
}

type MinHeap []*ReadAndPrepareOp

func (pq MinHeap) Len() int {
	return len(pq)
}

func (pq MinHeap) Less(i, j int) bool {
	if pq[i].request.Timestamp < pq[j].request.Timestamp {
		return true
	} else if pq[i].request.Timestamp == pq[j].request.Timestamp {
		return pq[i].request.ClientId < pq[i].request.ClientId
	} else {
		return false
	}
}

func (pq MinHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *MinHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(*ReadAndPrepareOp)
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
