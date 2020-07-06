package server

import (
	"Carousel-GTS/utils"
	log "github.com/sirupsen/logrus"
	"time"
)

//type Scheduler interface {
//	Schedule(op ScheduleOperation)
//}
//
//type NoScheduler struct {
//	server *Server
//}

//func (s *NoScheduler) Schedule(op ScheduleOperation) {
//	readAndPrepareOp, ok := op.(*ReadAndPrepareGTS)
//	if !ok {
//		log.Fatalf("cannot convert to readAndPrepareOp")
//	}
//	s.server.executor.PrepareTxn <- readAndPrepareOp
//}

type Scheduler struct {
	server         *Server
	priorityQueue  *PriorityQueue
	pendingOp      chan ScheduleOperation
	timer          *time.Timer
	highPrioritySL *utils.SkipList
}

func NewScheduler(server *Server) *Scheduler {
	ts := &Scheduler{
		server:         server,
		priorityQueue:  NewPriorityQueue(),
		pendingOp:      make(chan ScheduleOperation, server.config.GetQueueLen()),
		timer:          time.NewTimer(0),
		highPrioritySL: utils.NewSkipList(),
	}

	go ts.run()
	return ts
}

func (ts *Scheduler) run() {
	for {
		select {
		case op := <-ts.pendingOp:
			op.Schedule(ts)
		case <-ts.timer.C:
			ts.resetTimer()
		}
	}
}

func conflict(low *ReadAndPrepareGTS, high *ReadAndPrepareGTS) bool {
	for _, rk := range low.request.Txn.ReadKeyList {
		if _, exist := high.allWriteKeys[rk]; exist {
			log.Debugf("key %v : txn (low) %v read and txn (high) %v write", rk, low.txnId, high.txnId)
			return true
		}
	}

	for wk := range low.allWriteKeys {
		if _, exist := high.allWriteKeys[wk]; exist {
			return true
		}
		if _, exist := high.allReadKeys[wk]; exist {
			return true
		}
	}

	return false
}

func (ts *Scheduler) checkConflictWithHighPriorityTxn(op *ReadAndPrepareGTS) {
	// get high priority txn >= low priority txn timestamp
	cur := ts.highPrioritySL.Search(op, op.request.Timestamp)

	for cur != nil {
		if cur.V == nil {
			cur = cur.Forwards[0]
			continue
		}
		// if the high priority txn has smaller timestamp, then check the next one
		// the low priority does not affect the high priority
		if cur.Score < op.request.Timestamp {
			cur = cur.Forwards[0]
			continue
		}

		// if the time between execution the low and high priority txn < specified window
		// and they have the conflict, we abort the low priority txn
		duration := time.Duration(cur.Score - op.request.Timestamp)
		if duration <= ts.server.config.GetTimeWindow() {
			if conflict(op, cur.V.(*ReadAndPrepareGTS)) {
				op.selfAbort = true
				break
			}
		} else {
			// if over the time window, break
			break
		}
		cur = cur.Forwards[0]
	}
}

func (ts *Scheduler) resetTimer() {
	nextOp := ts.priorityQueue.Peek()
	for nextOp != nil {
		nextTime := nextOp.request.Timestamp
		diff := nextTime - time.Now().UnixNano()
		if diff <= 0 {
			op := ts.priorityQueue.Pop()
			if ts.server.config.GetPriority() && ts.server.config.IsEarlyAbort() {
				if op.request.Txn.HighPriority {
					ts.highPrioritySL.Delete(op, op.request.Timestamp)
				} else {
					ts.checkConflictWithHighPriorityTxn(op)
				}
			}
			ts.server.storage.AddOperation(op)
		} else {
			ts.timer.Reset(time.Duration(diff))
			break
		}
		nextOp = ts.priorityQueue.Peek()
	}
}

func (ts *Scheduler) AddOperation(op ScheduleOperation) {
	ts.pendingOp <- op
}
