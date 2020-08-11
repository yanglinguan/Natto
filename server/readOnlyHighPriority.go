package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadOnlyHighPriority struct {
	*ReadAndPrepareHighPriority
}

func NewReadOnlyHighPriority(request *rpc.ReadAndPrepareRequest, server *Server) *ReadOnlyHighPriority {
	return &ReadOnlyHighPriority{NewReadAndPrepareHighPriority(request, server)}
}

func (r *ReadOnlyHighPriority) Execute(storage *Storage) {
	logrus.Debugf("txn %v start execute timestamp %v idx %v",
		r.txnId, r.request.Timestamp, r.index)
	if !storage.server.config.GetIsReadOnly() {
		r.ReadAndPrepareHighPriority.Execute(storage)
		return
	}

	storage.AddTxn(r)

	available := storage.checkKeysAvailable(r)
	waiting := storage.hasWaitingTxn(r)

	if available && !waiting {
		storage.setReadResult(r, PREPARED, true)
	} else {
		if r.IsPassTimestamp() {
			storage.setReadResult(r, PASS_TIMESTAMP_ABORT, true)
			return
		}
		storage.wait(r)
	}
}

func (r *ReadOnlyHighPriority) executeFromQueue(storage *Storage) bool {
	waiting := storage.hasWaitingTxn(r)
	if waiting {
		logrus.Debugf("txn %v still has txn before it, wait", r.txnId)
		return false
	}

	available := storage.checkKeysAvailable(r)

	if !available {
		logrus.Debugf("txn %v keys are not available, wait", r.txnId)
		return false
	}
	storage.setReadResult(r, PREPARED, true)
	storage.removeFromQueue(r)

	releaseOp := NewRelease(r.txnId)
	storage.AddOperation(releaseOp)

	return true
}
