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

func (q *PriorityQueue) Pop() LockingOp {
	return heap.Pop(&q.minHeap).(LockingOp)
}

func (q *PriorityQueue) Peek() LockingOp {
	if q.minHeap.Len() == 0 {
		return nil
	}

	return q.minHeap[0]
}

func (q *PriorityQueue) Len() int {
	return q.minHeap.Len()
}

func (q *PriorityQueue) Push(op LockingOp) {
	heap.Push(&q.minHeap, op)
}

func (q *PriorityQueue) Remove(op LockingOp) {
	for i := 0; i < len(q.minHeap); i++ {
		if q.minHeap[i].GetTxnId() == op.GetTxnId() {
			q.minHeap[i], q.minHeap[len(q.minHeap)-1] = q.minHeap[len(q.minHeap)-1], q.minHeap[i]
			q.minHeap = q.minHeap[:len(q.minHeap)-1]
			break
		}
	}

	heap.Init(&q.minHeap)
}

type MinHeap []LockingOp

func (pq MinHeap) Len() int {
	return len(pq)
}

func (pq MinHeap) Less(i, j int) bool {
	requestI := pq[i].GetReadRequest()
	requestJ := pq[j].GetReadRequest()
	if requestI.Timestamp == requestJ.Timestamp {
		return requestI.Txn.TxnId < requestJ.Txn.TxnId
	}
	return requestI.Timestamp < requestJ.Timestamp
}

func (pq MinHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].setIndex(i)
	pq[j].setIndex(j)
}

func (pq *MinHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(LockingOp)
	item.setIndex(n)
	*pq = append(*pq, item)
}

func (pq *MinHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil    // avoid memory leak
	item.setIndex(-1) // for safety
	*pq = old[0 : n-1]
	return item
}
