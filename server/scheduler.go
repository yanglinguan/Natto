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
	server         *Server
	priorityQueue  *PriorityQueue
	pendingOp      chan *ReadAndPrepareOp
	timer          *time.Timer
	highPrioritySL *SkipList
}

func NewTimestampScheduler(server *Server) *TimestampScheduler {
	ts := &TimestampScheduler{
		server:         server,
		priorityQueue:  NewPriorityQueue(),
		pendingOp:      make(chan *ReadAndPrepareOp, server.config.GetQueueLen()),
		timer:          time.NewTimer(0),
		highPrioritySL: NewSkipList(),
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

func conflict(low *ReadAndPrepareOp, high *ReadAndPrepareOp) bool {
	//log.Warnf("find conflict txn %v txn %v", low, high)
	log.Warnf("low txn %v readKey %v writeKey %v", low.txnId, low.allReadKeys, low.allWriteKeys)
	log.Warnf("high txn %v readKey %v writeKey %v", high.txnId, high.allReadKeys, high.allWriteKeys)
	for rk := range low.allReadKeys {
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

func (ts *TimestampScheduler) checkConflictWithHighPriorityTxn(op *ReadAndPrepareOp) {
	cur := ts.highPrioritySL.Search(op, op.request.Timestamp)

	//log.Warnf("txn %v : %v", op.txnId, cur.forwards[0])
	for cur != nil {
		if cur.v == nil {
			cur = cur.forwards[0]
			continue
		}
		//log.Warnf("here")
		// if the high priority txn has smaller timestamp, then check the next one
		// the low priority does not affect the high priority
		if cur.score < op.request.Timestamp {
			cur = cur.forwards[0]
			continue
		}

		//hTm := time.Unix(cur.forwards[0].score, 0)
		//lTm := time.Unix(op.request.Timestamp, 0)
		duration := time.Duration(cur.score - op.request.Timestamp)
		//duration := hTm.Sub(lTm)
		log.Warnf("here1: high txn %v and low txn %v duration %v, %v %v", cur.v.(*ReadAndPrepareOp).txnId, op.txnId, duration)
		if duration <= ts.server.config.GetTimeWindow() {
			log.Warnf("here2: txn %v within duration", op.txnId)
			if conflict(op, cur.v.(*ReadAndPrepareOp)) {
				log.Warnf("here3: txn %v self abort because of high priority txn %v",
					op.txnId, cur.v.(*ReadAndPrepareOp).txnId)
				op.selfAbort = true
				break
			}
		} else {
			// if over the time window, break
			break
		}
		cur = cur.forwards[0]
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
					ts.highPrioritySL.Delete(op, op.request.Timestamp)
				} else {
					ts.checkConflictWithHighPriorityTxn(op)
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
	if !op.request.Txn.HighPriority && !ts.server.config.GetAssignLowPriorityTimestamp() {
		if ts.server.config.GetPriority() && ts.server.config.GetTimeWindow() > 0 {
			ts.checkConflictWithHighPriorityTxn(op)
		}
		ts.server.executor.PrepareTxn <- op
		return
	}

	if op.request.Timestamp < time.Now().UnixNano() {
		log.Infof("PASS Current time %v", op.txnId)
		op.passedTimestamp = true
	}

	ts.priorityQueue.Push(op)
	if op.request.Txn.HighPriority && ts.server.config.GetTimeWindow() > 0 {
		ts.highPrioritySL.Insert(op, op.request.Timestamp)
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
