package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
	"time"
)

type ReadOnlyGTS struct {
	*ReadAndPrepareGTS
}

func NewReadOnlyGTS(request *rpc.ReadAndPrepareRequest, server *Server) *ReadOnlyGTS {
	return &ReadOnlyGTS{NewReadAndPrepareGTS(request, server)}
}

func (r *ReadOnlyGTS) Execute(storage *Storage) {
	log.Debugf("txn %v start execute timestamp %v", r.txnId, r.request.Timestamp)
	if !storage.server.config.GetIsReadOnly() {
		r.ReadAndPrepareGTS.Execute(storage)
		return
	}

	if r.GetPriority() {
		r.highPriorityExecute(storage)
	} else {
		r.lowPriorityExecute(storage)
	}
}

func (r *ReadOnlyGTS) Schedule(scheduler *Scheduler) {
	if r.request.Timestamp < time.Now().UnixNano() {
		log.Infof("PASS Current time %v", r.txnId)
		r.passedTimestamp = true
	}

	scheduler.priorityQueue.Push(r)
	if r.highPriority && scheduler.server.config.GetPriority() && scheduler.server.config.IsEarlyAbort() {
		scheduler.highPrioritySL.Insert(r, r.request.Timestamp)
	}
	if r.index == 0 {
		if !scheduler.timer.Stop() && len(scheduler.timer.C) > 0 {
			<-scheduler.timer.C
		}
		scheduler.resetTimer()
	}
}

func (r *ReadOnlyGTS) highPriorityExecute(storage *Storage) {
	storage.AddTxn(r)

	available := storage.checkKeysAvailable(r)
	waiting := storage.hasWaitingTxn(r)

	if available && !waiting {
		storage.setReadResult(r, PREPARED, true)
		//} else if available && storage.server.config.IsOptimisticReorder() {
		//		storage.setReadResult(r, PREPARED, true)
	} else {
		storage.wait(r)
	}

}

func (r *ReadOnlyGTS) lowPriorityExecute(storage *Storage) {
	log.Debugf("txn %v low priority read only execute selfAbort %v", r.txnId, r.selfAbort)
	storage.AddTxn(r)

	available := r.selfAbort && storage.checkKeysAvailable(r)
	status := PREPARED
	if !available {
		status = ABORT
	}

	if status == PREPARED {
		waiting := storage.hasWaitingTxn(r)
		if waiting {
			status = ABORT
		}
	}

	storage.setReadResult(r, status, true)
}

func (r *ReadOnlyGTS) executeFromQueue(storage *Storage) bool {
	waiting := storage.hasWaitingTxn(r)
	if waiting {
		log.Debugf("txn %v still has txn before it, wait", r.txnId)
		return false
	}

	available := storage.checkKeysAvailable(r)

	if !available {
		log.Debugf("txn %v keys are not available, wait", r.txnId)
		return false
	}
	storage.setReadResult(r, PREPARED, true)
	storage.removeFromQueue(r)

	releaseOp := NewReleaseReadOnly(r.txnId)
	storage.AddOperation(releaseOp)

	return true
}
