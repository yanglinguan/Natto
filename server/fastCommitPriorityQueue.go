package server

import (
	"container/heap"
)

type FCPriorityQueue struct {
	minHeap MinHeap
}

func NewFCPriorityQueue() *FCPriorityQueue {
	pq := &FCPriorityQueue{
		make(MinHeap, 0),
	}
	heap.Init(&pq.minHeap)
	return pq
}

func (q *FCPriorityQueue) Pop() FastCommitOpI {
	return heap.Pop(&q.minHeap).(FastCommitOpI)
}

func (q *FCPriorityQueue) Peek() FastCommitOpI {
	if q.minHeap.Len() == 0 {
		return nil
	}

	return q.minHeap[0]
}

func (q *FCPriorityQueue) Len() int {
	return q.minHeap.Len()
}

func (q *FCPriorityQueue) Push(op FastCommitOpI) {
	heap.Push(&q.minHeap, op)
}

func (q *FCPriorityQueue) Remove(op FastCommitOpI) {
	for i := 0; i < len(q.minHeap); i++ {
		if q.minHeap[i].GetTxnId() == op.GetTxnId() {
			q.minHeap[i], q.minHeap[len(q.minHeap)-1] = q.minHeap[len(q.minHeap)-1], q.minHeap[i]
			q.minHeap = q.minHeap[:len(q.minHeap)-1]
			break
		}
	}

	heap.Init(&q.minHeap)
}

type FCMinHeap []FastCommitOpI

func (pq FCMinHeap) Len() int {
	return len(pq)
}

func (pq FCMinHeap) Less(i, j int) bool {
	if pq[i].GetTimestamp() == pq[j].GetTimestamp() {
		return pq[i].GetTxnId() < pq[j].GetTxnId()
	}
	return pq[i].GetTimestamp() < pq[j].GetTimestamp()
}

func (pq FCMinHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].SetIndex(i)
	pq[j].SetIndex(j)
}

func (pq *FCMinHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(FastCommitOpI)
	item.SetIndex(n)
	*pq = append(*pq, item)
}

func (pq *FCMinHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil    // avoid memory leak
	item.SetIndex(-1) // for safety
	*pq = old[0 : n-1]
	return item
}
