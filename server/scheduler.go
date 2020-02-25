package server

import (
	"time"
)

type Scheduler interface {
	Schedule(op *ReadAndPrepareOp)
	//GetNextRequest() *ReadAndPrepareOp
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
		pendingOp:     make(chan *ReadAndPrepareOp, QueueLen),
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
	for nextOp := ts.priorityQueue.Peek(); nextOp != nil; {
		nextTime := nextOp.request.Timestamp
		diff := nextTime - time.Now().UnixNano()
		if diff <= 0 {
			ts.server.executor.PrepareTxn <- ts.priorityQueue.Pop()
		} else {
			ts.timer.Reset(time.Duration(diff))
			break
		}
	}
}

func (ts *TimestampScheduler) handleOp(op *ReadAndPrepareOp) {
	if op.request.Timestamp < time.Now().UnixNano() {
		ts.server.executor.AbortTxn <- &AbortRequestOp{
			abortRequest:      nil,
			request:           op,
			isFromCoordinator: false,
			sendToCoordinator: false,
		}
		return
	}

	ts.priorityQueue.Push(op)
	if op.index == 0 {
		if !ts.timer.Stop() {
			<-ts.timer.C
		}
		ts.resetTimer()
	}
}

func (ts *TimestampScheduler) Schedule(op *ReadAndPrepareOp) {
	ts.pendingOp <- op
}
