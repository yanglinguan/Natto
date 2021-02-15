package server

import (
	"container/list"
	"github.com/sirupsen/logrus"
)

type WaitingList interface {
	Push(op LockingOp)
	Front() LockingOp
	Remove(op LockingOp) bool
	Len() int

	InQueue(txnId string) bool
	GetWaitingItems() map[string]LockingOp
	Position(txnId string) int
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

func (q *Queue) Position(txnId string) int {
	head := q.waitingOp.Front()
	pos := 0
	for head != nil {
		if head.Value.(LockingOp).GetTxnId() == txnId {
			return pos
		}
		pos++
		head = head.Next()
	}
	logrus.Debugf("txn %v not in the queue", txnId)
	return pos
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

func (q *Queue) Remove(op LockingOp) bool {
	if _, exist := q.waitingItem[op.GetTxnId()]; !exist {
		return false
	}
	q.waitingOp.Remove(q.waitingItem[op.GetTxnId()])
	delete(q.waitingItem, op.GetTxnId())
	return true
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

func (q *PQueue) Position(txnId string) int {
	return q.waitingOp.Position(txnId)
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
	op, ok := q.waitingOp.Peek().(LockingOp)
	if !ok {
		logrus.Fatalf("should be convert to locking op")
	}
	return op
}

func (q *PQueue) Remove(op LockingOp) bool {
	if _, exist := q.waitingItem[op.GetTxnId()]; !exist {
		return false
	}

	q.waitingOp.Remove(op)
	delete(q.waitingItem, op.GetTxnId())
	return true
}

func (q *PQueue) Len() int {
	return q.waitingOp.Len()
}
