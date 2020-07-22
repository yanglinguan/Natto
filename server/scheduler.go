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
//	readRequest, ok := op.(*ReadAndPrepareGTS)
//	if !ok {
//		log.Fatalf("cannot convert to readRequest")
//	}
//	s.server.executor.PrepareTxn <- readRequest
//}

type Scheduler struct {
	server         *Server
	priorityQueue  *PriorityQueue
	pendingOp      chan GTSOp
	timer          *time.Timer
	highPrioritySL *utils.SkipList
}

func NewScheduler(server *Server) *Scheduler {
	ts := &Scheduler{
		server:         server,
		priorityQueue:  NewPriorityQueue(),
		pendingOp:      make(chan GTSOp, server.config.GetQueueLen()),
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

func conflict(low GTSOp, high GTSOp) bool {
	for rk := range low.GetAllReadKeys() {
		if _, exist := high.GetAllWriteKeys()[rk]; exist {
			log.Debugf("key %v : txn (low) %v read and txn (high) %v write",
				rk, low.GetTxnId(), high.GetTxnId())
			return true
		}
	}

	for wk := range low.GetAllWriteKeys() {
		if _, exist := high.GetAllWriteKeys()[wk]; exist {
			return true
		}
		if _, exist := high.GetAllReadKeys()[wk]; exist {
			return true
		}
	}

	return false
}

func (ts *Scheduler) checkConflictWithHighPriorityTxn(op GTSOp) {
	// get high priority txn >= low priority txn timestamp
	cur := ts.highPrioritySL.Search(op, op.GetReadRequest().Timestamp)

	for cur != nil {
		if cur.V == nil {
			cur = cur.Forwards[0]
			continue
		}
		// if the high priority txn has smaller timestamp, then check the next one
		// the low priority does not affect the high priority
		if cur.Score < op.GetReadRequest().Timestamp {
			cur = cur.Forwards[0]
			continue
		}

		// if the time between execution the low and high priority txn < specified window
		// and they have the conflict, we abort the low priority txn
		duration := time.Duration(cur.Score - op.GetReadRequest().Timestamp)
		if duration <= ts.server.config.GetTimeWindow() {
			if conflict(op, cur.V.(GTSOp)) {
				op.setSelfAbort()
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
		nextTime := nextOp.GetReadRequest().Timestamp
		diff := nextTime - time.Now().UnixNano()
		if diff <= 0 {
			op, ok := ts.priorityQueue.Pop().(GTSOp)
			if !ok {
				log.Fatalf("txn %v should be convert to GTSOP only GTS should schedule", op.GetTxnId())
			}
			if ts.server.config.GetPriority() && ts.server.config.IsEarlyAbort() {
				if op.GetReadRequest().Txn.HighPriority {
					ts.highPrioritySL.Delete(op, op.GetReadRequest().Timestamp)
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

func (ts *Scheduler) AddOperation(op GTSOp) {
	ts.pendingOp <- op
}
