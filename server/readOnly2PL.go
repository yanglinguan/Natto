package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadOnly2PL struct {
	*ReadAndPrepare2PL
}

func NewReadOnly2PL(request *rpc.ReadAndPrepareRequest) *ReadOnly2PL {
	return &ReadOnly2PL{NewReadAndPrepareLock2PL(request)}
}

func (o *ReadOnly2PL) Execute(storage *Storage) {
	logrus.Debugf("txn %v start execute timestamp %v",
		o.txnId, o.request.Timestamp)
	if !storage.server.config.GetIsReadOnly() {
		o.ReadAndPrepare2PL.Execute(storage)
		return
	}

	storage.AddTxn(o)

	available := storage.checkKeysAvailable(o)
	waiting := storage.hasWaitingTxn(o)

	if storage.server.config.UseNetworkTimestamp() {
		if available && !waiting {
			storage.setReadResult(o, PREPARED, true)
			return
		}

		if o.IsPassTimestamp() {
			storage.setReadResult(o, PASS_TIMESTAMP_ABORT, true)
			return
		}
		storage.wait(o)
		return
	}

	if available && !waiting {
		storage.setReadResult(o, PREPARED, true)
	} else if available {
		if storage.isOldest(o.ReadAndPrepare2PL) {
			storage.setReadResult(o, PREPARED, true)
		} else {
			storage.wait(o)
		}
	} else {
		if storage.hasYoungerPrepare(o.ReadAndPrepare2PL) {
			storage.setReadResult(o, WOUND_ABORT, true)
		} else {
			storage.wait(o)
		}
	}
}

func (o *ReadOnly2PL) executeFromQueue(storage *Storage) bool {
	waiting := storage.hasWaitingTxn(o)
	if waiting {
		logrus.Debugf("txn %v cannot prepare because should has older txn before it", o.txnId)
		return false
	}

	available := storage.checkKeysAvailable(o)
	if !available {
		logrus.Debugf("txn %v cannot prepare because key are not available", o.txnId)
		return false
	}
	storage.setReadResult(o, PREPARED, false)
	storage.removeFromQueue(o)
	releaseOp := NewReleaseReadOnly(o.txnId)
	storage.AddOperation(releaseOp)
	//storage.prepare(o)
	return true
}
