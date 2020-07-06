package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type ReadOnlyGTS struct {
	*ReadAndPrepareGTS
}

func NewReadOnlyGTS(request *rpc.ReadAndPrepareRequest, server *Server) *ReadOnlyGTS {
	return &ReadOnlyGTS{NewReadAndPrepareGTS(request, server)}
}

func (r *ReadOnlyGTS) Execute(storage *Storage) {
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
	scheduler.server.storage.AddOperation(r)
}

func (r *ReadOnlyGTS) highPriorityExecute(storage *Storage) {

}

func (r *ReadOnlyGTS) lowPriorityExecute(storage *Storage) {
	log.Debugf("txn %v low priority read only execute selfAbort %v", r.txnId, r.selfAbort)
	available := r.selfAbort && storage.checkKeysAvailable(r)
	status := PREPARED
	if !available {
		status = ABORT
	}

	storage.setReadResult(r, status, true)
}
