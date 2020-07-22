package server

import (
	"container/list"
)

type WaitingList interface {
	Push(op LockingOp)
	Front() LockingOp
	Remove(op LockingOp)
	Len() int

	InQueue(txnId string) bool
	GetWaitingItems() map[string]LockingOp
}

type Queue struct {
	waitingOp   *list.List
	waitingItem map[string]*list.Element
}

func NewQueue() *Queue {
	q := &Queue{
		waitingOp:   list.New(),
		waitingItem: make(map[string]*list.Element),
	}
	return q
}

func (q *Queue) InQueue(txnId string) bool {
	_, exist := q.waitingItem[txnId]
	return exist
}

func (q *Queue) GetWaitingItems() map[string]LockingOp {

	return make(map[string]LockingOp)
}

func (q *Queue) Push(op LockingOp) {
	item := q.waitingOp.PushBack(op)
	q.waitingItem[op.GetTxnId()] = item
}

func (q *Queue) Front() LockingOp {
	front := q.waitingOp.Front()
	if front == nil {
		return nil
	}
	return q.waitingOp.Front().Value.(LockingOp)
}

func (q *Queue) Remove(op LockingOp) {
	if _, exist := q.waitingItem[op.GetTxnId()]; !exist {
		return
	}
	q.waitingOp.Remove(q.waitingItem[op.GetTxnId()])
	delete(q.waitingItem, op.GetTxnId())
}

func (q *Queue) Len() int {
	return q.waitingOp.Len()
}

type PQueue struct {
	waitingOp   *PriorityQueue
	waitingItem map[string]LockingOp
}

func NewPQueue() *PQueue {
	q := &PQueue{
		waitingOp:   NewPriorityQueue(),
		waitingItem: make(map[string]LockingOp),
	}
	return q
}

func (q *PQueue) InQueue(txnId string) bool {
	_, exist := q.waitingItem[txnId]
	return exist
}

func (q *PQueue) GetWaitingItems() map[string]LockingOp {
	return q.waitingItem
}

func (q *PQueue) Push(op LockingOp) {
	q.waitingOp.Push(op)
	q.waitingItem[op.GetTxnId()] = op
}

func (q *PQueue) Front() LockingOp {
	return q.waitingOp.Peek()
}

func (q *PQueue) Remove(op LockingOp) {
	if _, exist := q.waitingItem[op.GetTxnId()]; !exist {
		return
	}

	q.waitingOp.Remove(op)
	delete(q.waitingItem, op.GetTxnId())
}

func (q *PQueue) Len() int {
	return q.waitingOp.Len()
}
