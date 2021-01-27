package server

import (
	"Carousel-GTS/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

type Scheduler struct {
	server         *Server
	priorityQueue  *PriorityQueue
	pendingOp      chan ReadAndPrepareOp
	timer          *time.Timer
	highPrioritySL *utils.SkipList

	curTxn string
}

func NewScheduler(server *Server) *Scheduler {
	ts := &Scheduler{
		server:         server,
		priorityQueue:  NewPriorityQueue(),
		pendingOp:      make(chan ReadAndPrepareOp, server.config.GetQueueLen()),
		timer:          time.NewTimer(0),
		highPrioritySL: utils.NewSkipList(),
		curTxn:         "0-1-0",
	}

	go ts.run()
	return ts
}

func (ts *Scheduler) run() {
	for {
		select {
		case op := <-ts.pendingOp:
			ts.Schedule(op)
			//op.Schedule(ts)
			//case <-ts.timer.C:
			//default:
			//	ts.resetTimer()
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

func (ts *Scheduler) checkConflictWithHighPriorityTxn(op PriorityOp) {
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
		log.Debugf("top of the pq %v curTxn %v",
			nextOp.GetTxnId(), ts.curTxn)

		if nextOp.GetTxnId() != ts.curTxn {
			break
		}
		op := ts.priorityQueue.Pop()
		ts.server.storage.AddOperation(op)
		nextOp = ts.priorityQueue.Peek()
		ts.getNextTxn()
	}
}

func (ts *Scheduler) getNextTxn() {
	items := strings.Split(ts.curTxn, "-")
	cId, _ := strconv.Atoi(items[0])
	tId, _ := strconv.Atoi(items[1])
	totalPartition := ts.server.config.GetTotalPartition()
	tIdStr := strconv.Itoa(tId + 1)
	if (tId-1)%totalPartition == 0 {
		if cId == totalPartition-1 {
			ts.curTxn = strconv.Itoa(ts.server.partitionId) +
				"-" + tIdStr + "-0"
		} else {
			ts.curTxn = strconv.Itoa(cId+1) +
				"-" + strconv.Itoa(tId) + "-0"
		}
	} else {
		if tId%totalPartition == 0 {
			ts.curTxn = "0-" + tIdStr + "-0"
		} else {
			ts.curTxn = strconv.Itoa(ts.server.partitionId) +
				"-" + tIdStr + "-0"
		}
	}
	log.Debugf("gen next txn: %v", ts.curTxn)
}

//func (ts *Scheduler) resetTimer() {
//	nextOp := ts.priorityQueue.Peek()
//	for nextOp != nil {
//		nextTime := nextOp.GetReadRequest().Timestamp
//		diff := nextTime - time.Now().UnixNano()
//		if diff <= 0 {
//			op := ts.priorityQueue.Pop()
//			if ts.server.config.GetServerMode() == configuration.PRIORITY &&
//				ts.server.config.IsEarlyAbort() {
//				gtsOp, ok := op.(PriorityOp)
//				if !ok {
//					log.Fatalf("txn %v should be convert to gts op", gtsOp.GetTxnId())
//				}
//				if op.GetPriority() {
//					ts.highPrioritySL.Delete(gtsOp, gtsOp.GetTimestamp())
//				} else {
//					ts.checkConflictWithHighPriorityTxn(gtsOp)
//				}
//			}
//			ts.server.storage.AddOperation(op)
//		} else {
//			ts.timer.Reset(time.Duration(diff))
//			break
//		}
//		nextOp = ts.priorityQueue.Peek()
//	}
//}

func (ts *Scheduler) AddOperation(op ReadAndPrepareOp) {
	ts.pendingOp <- op
}

func (ts *Scheduler) Schedule(op ReadAndPrepareOp) {
	prob, ok := op.(*ProbeOp)
	if ok {
		prob.Execute(nil)
		return
	}
	log.Debugf("txn %v push to pq", op.GetTxnId())
	ts.priorityQueue.Push(op)
	ts.resetTimer()
	//if op.GetTimestamp() < time.Now().UnixNano() {
	//	log.Debugf("txn %v PASS Current time %v", op.GetTxnId(), op.GetTimestamp())
	//	op.SetPassTimestamp()
	//}
	//
	//ts.priorityQueue.Push(op)
	//if ts.server.config.GetServerMode() == configuration.PRIORITY &&
	//	op.GetPriority() &&
	//	ts.server.config.IsEarlyAbort() {
	//	ts.highPrioritySL.Insert(op, op.GetTimestamp())
	//}
	//if op.GetIndex() == 0 {
	//	if !ts.timer.Stop() && len(ts.timer.C) > 0 {
	//		<-ts.timer.C
	//	}
	//	ts.resetTimer()
	//}
}
