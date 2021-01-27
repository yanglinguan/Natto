package server

import (
	"container/heap"
	"strconv"
	"strings"
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

func (q *PriorityQueue) Pop() ReadAndPrepareOp {
	return heap.Pop(&q.minHeap).(ReadAndPrepareOp)
}

func (q *PriorityQueue) Peek() ReadAndPrepareOp {
	if q.minHeap.Len() == 0 {
		return nil
	}

	return q.minHeap[0]
}

func (q *PriorityQueue) Len() int {
	return q.minHeap.Len()
}

func (q *PriorityQueue) Push(op ReadAndPrepareOp) {
	heap.Push(&q.minHeap, op)
}

func (q *PriorityQueue) Remove(op ReadAndPrepareOp) {
	for i := 0; i < len(q.minHeap); i++ {
		if q.minHeap[i].GetTxnId() == op.GetTxnId() {
			q.minHeap[i], q.minHeap[len(q.minHeap)-1] = q.minHeap[len(q.minHeap)-1], q.minHeap[i]
			q.minHeap = q.minHeap[:len(q.minHeap)-1]
			break
		}
	}

	heap.Init(&q.minHeap)
}

type MinHeap []ReadAndPrepareOp

func (pq MinHeap) Len() int {
	return len(pq)
}

func (pq MinHeap) Less(i, j int) bool {
	itemsI := strings.Split(pq[i].GetTxnId(), "-")
	cIdI, _ := strconv.Atoi(itemsI[0])
	tIdI, _ := strconv.Atoi(itemsI[1])

	itemsJ := strings.Split(pq[j].GetTxnId(), "-")
	cIdJ, _ := strconv.Atoi(itemsJ[0])
	tIdJ, _ := strconv.Atoi(itemsJ[1])
	if tIdI == tIdJ {
		return cIdI < cIdJ
	}
	return tIdI < tIdJ

	//if pq[i].GetTimestamp() == pq[j].GetTimestamp() {
	//	return pq[i].GetTxnId() < pq[j].GetTxnId()
	//}
	//return pq[i].GetTimestamp() < pq[j].GetTimestamp()
}

func (pq MinHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].SetIndex(i)
	pq[j].SetIndex(j)
}

func (pq *MinHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(ReadAndPrepareOp)
	item.SetIndex(n)
	*pq = append(*pq, item)
}

func (pq *MinHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil    // avoid memory leak
	item.SetIndex(-1) // for safety
	*pq = old[0 : n-1]
	return item
}
