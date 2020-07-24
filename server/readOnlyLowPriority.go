package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadOnlyLowPriority struct {
	*ReadAndPrepareLowPriority
}

func NewReadOnlyLowPriority(request *rpc.ReadAndPrepareRequest, server *Server) *ReadOnlyLowPriority {
	return &ReadOnlyLowPriority{NewReadAndPrepareLowPriority(request, server)}
}

func (r *ReadOnlyLowPriority) Execute(storage *Storage) {
	logrus.Debugf("txn %v start execute timestamp %v idx %v",
		r.txnId, r.request.Timestamp, r.index)
	if !storage.server.config.GetIsReadOnly() {
		r.ReadAndPrepareLowPriority.Execute(storage)
		return
	}

	logrus.Debugf("txn %v low priority read only execute selfAbort %v", r.txnId, r.selfAbort)
	storage.AddTxn(r)

	if r.selfAbort {
		storage.setReadResult(r, EARLY_ABORT, true)
		return
	}

	waiting := storage.hasWaitingTxn(r)
	if waiting {
		storage.setReadResult(r, WAITING_ABORT, true)
		return
	}

	available := storage.checkKeysAvailable(r)
	if !available {
		storage.setReadResult(r, CONFLICT_ABORT, true)
		return
	}

	storage.setReadResult(r, PREPARED, true)
}
