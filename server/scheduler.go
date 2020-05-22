package server

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type Scheduler interface {
	Schedule(op *ReadAndPrepareOp)
}

type NoScheduler struct {
	server *Server
}

func (s *NoScheduler) Schedule(op *ReadAndPrepareOp) {
	s.server.executor.PrepareTxn <- op
}

type TimestampScheduler struct {
	server          *Server
	priorityQueue   *PriorityQueue
	pendingOp       chan *ReadAndPrepareOp
	timer           *time.Timer
	highPriorityBST *BinarySearchTree
}

func NewTimestampScheduler(server *Server) *TimestampScheduler {
	ts := &TimestampScheduler{
		server:          server,
		priorityQueue:   NewPriorityQueue(),
		pendingOp:       make(chan *ReadAndPrepareOp, server.config.GetQueueLen()),
		timer:           time.NewTimer(0),
		highPriorityBST: NewBinarySearchTree(server.config.GetTimeWindow()),
	}

	go ts.run()
	return ts
}

func (ts *TimestampScheduler) run() {
	for {
		select {
		case op := <-ts.pendingOp:
			ts.handleOp(op)
		case <-ts.timer.C:
			ts.resetTimer()
		}
	}
}

func (ts *TimestampScheduler) resetTimer() {
	nextOp := ts.priorityQueue.Peek()
	for nextOp != nil {
		nextTime := nextOp.request.Timestamp
		diff := nextTime - time.Now().UnixNano()
		if diff <= 0 {
			op := ts.priorityQueue.Pop()
			if ts.server.config.GetPriority() && ts.server.config.GetTimeWindow() > 0 {
				if op.request.Txn.HighPriority {
					ts.highPriorityBST.Remove(op)
				} else {
					if ts.highPriorityBST.SearchConflictTxnWithinTimeWindow(op) {
						log.Warnf("txn %v low priority abort because high priority")
						op.selfAbort = true
					}
				}
			}
			ts.server.executor.PrepareTxn <- op
		} else {
			ts.timer.Reset(time.Duration(diff))
			break
		}
		nextOp = ts.priorityQueue.Peek()
	}
}

func (ts *TimestampScheduler) handleOp(op *ReadAndPrepareOp) {
	if op.request.Timestamp < time.Now().UnixNano() {
		log.Infof("PASS Current time %v", op.txnId)
		op.passedTimestamp = true
		//ts.server.executor.AbortTxn <- NewAbortRequestOp(nil, op, false)
		//return
	}

	ts.priorityQueue.Push(op)
	if op.request.Txn.HighPriority && ts.server.config.GetTimeWindow() > 0 {
		ts.highPriorityBST.Insert(op)
	}
	if op.index == 0 {
		if !ts.timer.Stop() && len(ts.timer.C) > 0 {
			<-ts.timer.C
		}
		ts.resetTimer()
	}
}

func (ts *TimestampScheduler) Schedule(op *ReadAndPrepareOp) {
	//if !op.request.Txn.HighPriority {
	//	log.Debugf("low priority txn %v do not need schedule", op.txnId)
	//	ts.server.executor.PrepareTxn <- op
	//	return
	//}

	ts.pendingOp <- op
}
