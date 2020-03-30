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
	server        *Server
	priorityQueue *PriorityQueue
	pendingOp     chan *ReadAndPrepareOp
	timer         *time.Timer
}

func NewTimestampScheduler(server *Server) *TimestampScheduler {
	ts := &TimestampScheduler{
		server:        server,
		priorityQueue: NewPriorityQueue(),
		pendingOp:     make(chan *ReadAndPrepareOp, server.config.GetQueueLen()),
		timer:         time.NewTimer(0),
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
			ts.server.executor.PrepareTxn <- ts.priorityQueue.Pop()
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
	if op.index == 0 {
		if !ts.timer.Stop() && len(ts.timer.C) > 0 {
			<-ts.timer.C
		}
		ts.resetTimer()
	}
}

func (ts *TimestampScheduler) Schedule(op *ReadAndPrepareOp) {
	ts.pendingOp <- op
}
