package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadAndPrepareHighPriority struct {
	*ReadAndPreparePriority
}

func NewReadAndPrepareHighPriority(request *rpc.ReadAndPrepareRequest, server *Server) *ReadAndPrepareHighPriority {
	return &ReadAndPrepareHighPriority{NewReadAndPreparePriority(request, server)}
}

func (o *ReadAndPrepareHighPriority) Execute(storage *Storage) {
	logrus.Debugf("txn %v execute high priority", o.txnId)

	if storage.checkAbort(o) {
		logrus.Debugf("txn %v high prioirty txn already abort", o.txnId)
		storage.setReadResult(o, -1, false)
		return
	}

	storage.AddTxn(o)

	available := storage.checkKeysAvailable(o)
	waiting := storage.hasWaitingTxn(o)

	if available && !waiting {
		storage.setReadResult(o, -1, false)
		storage.prepare(o)
	} else if available {
		storage.reorderPrepare(o)
	} else if !waiting {
		storage.conditionalPrepare(o)
	} else {
		if o.IsPassTimestamp() {
			storage.setReadResult(o, -1, false)
			storage.selfAbort(o, PASS_TIMESTAMP_ABORT)
			return
		}

		storage.wait(o)
	}
}

func (o *ReadAndPrepareHighPriority) executeFromQueue(storage *Storage) bool {

	waiting := storage.hasWaitingTxn(o)
	if waiting {
		logrus.Debugf("txn %v still has txn before it, wait", o.txnId)
		return false
	}

	available, reorderTxn := storage.checkKeysAvailableFromQueue(o)

	if available && len(reorderTxn) == 0 {
		storage.setReadResult(o, -1, false)
		storage.prepare(o)
	} else if available {
		storage.setReadResult(o, -1, false)
		storage.reverseReorderPrepare(o, reorderTxn)
	} else {
		return false
	}

	storage.removeFromQueue(o)

	return true
}
