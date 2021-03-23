package server

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/utils"
	log "github.com/sirupsen/logrus"
	"time"
)

type Scheduler interface {
	AddOperation(op ReadAndPrepareOp)
	Schedule(op ReadAndPrepareOp)
}

type NoScheduler struct {
	server    *Server
	pendingOp chan ReadAndPrepareOp
}

func NewNoScheduler(server *Server) *NoScheduler {
	ns := &NoScheduler{
		server: server,
		pendingOp: make(chan ReadAndPrepareOp,
			server.config.GetQueueLen()),
	}
	go ns.run()
	return ns
}

func (ns *NoScheduler) run() {
	for {
		op := <-ns.pendingOp
		ns.Schedule(op)
	}
}

func (ns *NoScheduler) AddOperation(op ReadAndPrepareOp) {
	ns.pendingOp <- op
}

func (ns *NoScheduler) Schedule(op ReadAndPrepareOp) {
	if prob, ok := op.(*ProbeOp); ok {
		prob.Execute(nil)
		return
	}
	ns.server.storage.AddOperation(op)
}

type PriorityScheduler struct {
	lowTxn  chan ReadAndPrepareOp
	highTxn chan ReadAndPrepareOp
	pending chan bool
	server  *Server
}

func NewPriorityScheduler(server *Server) *PriorityScheduler {
	queueLen := server.config.GetQueueLen()
	ps := &PriorityScheduler{
		lowTxn:  make(chan ReadAndPrepareOp, queueLen),
		highTxn: make(chan ReadAndPrepareOp, queueLen),
		pending: make(chan bool, queueLen),
		server:  server,
	}
	go ps.run()
	return ps
}

func (ps *PriorityScheduler) AddOperation(op ReadAndPrepareOp) {
	if op.GetPriority() {
		ps.highTxn <- op
	} else {
		ps.lowTxn <- op
	}
	ps.pending <- true
}

func (ps *PriorityScheduler) run() {
	for {
		<-ps.pending
		var op ReadAndPrepareOp
		if len(ps.highTxn) > 0 {
			op = <-ps.highTxn
		} else {
			op = <-ps.lowTxn
		}
		ps.Schedule(op)
	}
}

func (ps *PriorityScheduler) Schedule(op ReadAndPrepareOp) {
	if prob, ok := op.(*ProbeOp); ok {
		prob.Execute(nil)
		return
	}
	ps.server.storage.AddOperation(op)
}

type TimestampScheduler struct {
	server         *Server
	priorityQueue  *PriorityQueue
	pendingOp      chan ReadAndPrepareOp
	timer          *time.Timer
	highPrioritySL *utils.SkipList
}

func NewTimestampScheduler(server *Server) *TimestampScheduler {
	ts := &TimestampScheduler{
		server:         server,
		priorityQueue:  NewPriorityQueue(),
		pendingOp:      make(chan ReadAndPrepareOp, server.config.GetQueueLen()),
		timer:          time.NewTimer(0),
		highPrioritySL: utils.NewSkipList(),
	}

	go ts.run()
	return ts
}

func (ts *TimestampScheduler) run() {
	for {
		select {
		case op := <-ts.pendingOp:
			ts.Schedule(op)
		case <-ts.timer.C:
			ts.resetTimer()
		}
	}
}

func conflict(low PriorityOp, high PriorityOp) bool {
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

func (ts *TimestampScheduler) checkConflictWithHighPriorityTxn(op PriorityOp) {
	// get high priority txn >= low priority txn timestamp
	cur := ts.highPrioritySL.Search(op, op.GetTimestamp())

	for cur != nil {
		if cur.V == nil {
			cur = cur.Forwards[0]
			continue
		}
		// if the high priority txn has smaller timestamp, then check the next one
		// the low priority does not affect the high priority
		if cur.Score < op.GetTimestamp() {
			cur = cur.Forwards[0]
			continue
		}

		// if the time between execution the low and high priority txn < specified window
		// and they have the conflict, we abort the low priority txn
		duration := time.Duration(cur.Score - op.GetTimestamp())
		if duration <= ts.server.config.GetTimeWindow() {
			if conflict(op, cur.V.(PriorityOp)) {
				cur.V.(*ReadAndPrepareHighPriority).hasEarlyAbort = true
				op.setSelfAbort()
				cur = cur.Forwards[0]
				for cur != nil {
					cur.V.(*ReadAndPrepareHighPriority).hasEarlyAbort = true
				}
				break
			}
		} else {
			// if over the time window, break
			break
		}
		cur = cur.Forwards[0]
	}
}

func (ts *TimestampScheduler) resetTimer() {
	nextOp := ts.priorityQueue.Peek()
	for nextOp != nil {
		nextTime := nextOp.GetReadRequest().Timestamp
		diff := nextTime - time.Now().UnixNano()
		if diff <= 0 {
			op := ts.priorityQueue.Pop()
			if ts.server.config.GetServerMode() == configuration.PRIORITY &&
				ts.server.config.IsEarlyAbort() {
				gtsOp, ok := op.(PriorityOp)
				if !ok {
					log.Fatalf("txn %v should be convert to gts op", gtsOp.GetTxnId())
				}
				if op.GetPriority() {
					ts.highPrioritySL.Delete(gtsOp, gtsOp.GetTimestamp())
				} else {
					ts.checkConflictWithHighPriorityTxn(gtsOp)
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

func (ts *TimestampScheduler) AddOperation(op ReadAndPrepareOp) {
	ts.pendingOp <- op
}

func (ts *TimestampScheduler) Schedule(op ReadAndPrepareOp) {
	if prob, ok := op.(*ProbeOp); ok {
		prob.Execute(nil)
		return
	}

	if op.GetTimestamp() < time.Now().UnixNano() {
		log.Debugf("txn %v PASS Current time %v", op.GetTxnId(), op.GetTimestamp())
		op.SetPassTimestamp()
	}

	log.Debugf("txn %v push to pq", op.GetTxnId())
	ts.priorityQueue.Push(op)
	if ts.server.config.GetServerMode() == configuration.PRIORITY &&
		op.GetPriority() &&
		ts.server.config.IsEarlyAbort() {
		ts.highPrioritySL.Insert(op, op.GetTimestamp())
	}
	if op.GetIndex() == 0 {
		if !ts.timer.Stop() && len(ts.timer.C) > 0 {
			<-ts.timer.C
		}
		ts.resetTimer()
	}
}
