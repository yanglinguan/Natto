package server

import (
	"Carousel-GTS/utils"
	"time"
)

type CommitScheduler struct {
	server         *Server
	priorityQueue  *FCPriorityQueue
	pendingOp      chan FastCommitOpI
	timer          *time.Timer
	highPrioritySL *utils.SkipList
}

func NewCommitScheduler(server *Server) *CommitScheduler {
	ts := &CommitScheduler{
		server:         server,
		priorityQueue:  NewFCPriorityQueue(),
		pendingOp:      make(chan FastCommitOpI, server.config.GetQueueLen()),
		timer:          time.NewTimer(0),
		highPrioritySL: utils.NewSkipList(),
	}

	go ts.run()
	return ts
}

func (ts *CommitScheduler) run() {
	for {
		select {
		case op := <-ts.pendingOp:
			ts.Schedule(op)
			//op.Schedule(ts)
		case <-ts.timer.C:
			ts.resetTimer()
		}
	}
}

func (ts *CommitScheduler) resetTimer() {
	nextOp := ts.priorityQueue.Peek()
	for nextOp != nil {
		nextTime := nextOp.GetTimestamp()
		diff := nextTime - time.Now().UnixNano()
		if diff <= 0 {
			op := ts.priorityQueue.Pop()
			abort, ok := op.(*FastAbortOp)
			if ok {
				abortOp := ts.server.operationCreator.createAbortOp(abort.request.AbortRequest)
				ts.server.storage.AddOperation(abortOp)
			}
			commit, ok := op.(*FastCommitOp)
			if ok {
				commitOp := ts.server.operationCreator.createCommitOp(commit.request.CommitRequest)
				ts.server.storage.AddOperation(commitOp)
			}
		} else {
			ts.timer.Reset(time.Duration(diff))
			break
		}
		nextOp = ts.priorityQueue.Peek()
	}
}

func (ts *CommitScheduler) AddOperation(op FastCommitOpI) {
	ts.pendingOp <- op
}

func (ts *CommitScheduler) Schedule(op FastCommitOpI) {
	prob, ok := op.(*ProbeOp)
	if ok {
		prob.Execute(nil)
		return
	}

	ts.priorityQueue.Push(op)

	if op.GetIndex() == 0 {
		if !ts.timer.Stop() && len(ts.timer.C) > 0 {
			<-ts.timer.C
		}
		ts.resetTimer()
	}
}
