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
		logrus.Debugf("txn %v finish execute high priority", o.txnId)
		return
	}

	storage.AddTxn(o)

	available := storage.checkKeysAvailable(o)
	waiting := storage.hasWaitingTxn(o)

	if storage.server.config.UseNetworkTimestamp() {
		if available && !waiting {
			storage.setReadResult(o, -1, false)
			storage.prepare(o)
		} else {
			if o.IsPassTimestamp() {
				storage.setReadResult(o, -1, false)
				storage.selfAbort(o, PASS_TIMESTAMP_ABORT)
				logrus.Debugf("txn %v finish execute high priority", o.txnId)
				return
			}

			if available {
				logrus.Warnf("txn %v can be reorder", o.txnId)
				storage.reorderPrepare(o)
			} else if !waiting {
				storage.conditionalPrepare(o)
			} else {
				storage.wait(o)
			}
		}
		logrus.Debugf("txn %v finish execute high priority", o.txnId)
		return
	}

	if storage.hasYoungerPrepare(o.ReadAndPrepare2PL) {
		storage.setReadResult(o, -1, false)
		storage.selfAbort(o, WOUND_ABORT)
		logrus.Debugf("txn %v finish execute high priority", o.txnId)
		return
	}

	if available && !waiting {
		storage.setReadResult(o, -1, false)
		storage.prepare(o)
		logrus.Debugf("txn %v finish execute high priority", o.txnId)
		return
	}

	storage.wait(o)
	logrus.Debugf("txn %v finish execute high priority", o.txnId)
}

func (o *ReadAndPrepareHighPriority) executeFromQueue(storage *Storage) bool {

	if storage.server.config.UseNetworkTimestamp() {
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
			logrus.Debugf("txn %v cannot prepare from queue; available %v, reorderTxn %v",
				o.txnId, available, reorderTxn)
			return false
		}

		storage.removeFromQueue(o)

		return true
	}

	if storage.hasYoungerPrepare(o.ReadAndPrepare2PL) {
		storage.setReadResult(o, -1, false)
		storage.selfAbort(o, WOUND_ABORT)
		storage.removeFromQueue(o)
		releaseOp := NewRelease(o.txnId)
		storage.AddOperation(releaseOp)
		return true
	}

	waiting := storage.hasWaitingTxn(o)
	available := storage.checkKeysAvailable(o)
	if available && !waiting {
		storage.setReadResult(o, -1, false)
		storage.removeFromQueue(o)
		storage.prepare(o)
		return true
	}

	return false

}
