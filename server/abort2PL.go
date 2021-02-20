package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type Abort2PL struct {
	abortRequest *rpc.AbortRequest
}

func NewAbort2PL(abortRequest *rpc.AbortRequest) *Abort2PL {
	a := &Abort2PL{
		abortRequest: abortRequest,
	}
	return a
}

func (a Abort2PL) Execute(storage *Storage) {
	txnId := a.abortRequest.TxnId
	if txnInfo, exist := storage.txnStore[txnId]; exist {
		txnInfo.receiveFromCoordinator = true
		if txnInfo.status.IsAbort() {
			logrus.Debugf("txn %v is already abort it self %v abort reason", txnId, txnInfo.status.String())
			//storage.releaseKeyAndCheckPrepare(txnId)
			return
		}
		switch txnInfo.status {
		case COMMIT:
			logrus.Fatalf("Error: txn %v is already committed", txnId)
			break
		default:
			logrus.Debugf("call abort processed txn %v", txnId)
			if storage.server.config.ReadBeforeCommitReplicate() {
				a.abortProcessedTxn(storage)
			} else {
				storage.replicateCommitResult(txnId,
					make([]*rpc.KeyValue, 0), COORDINATOR_ABORT)
			}
			break
		}
	} else {
		logrus.Debugf("ABORT %v (coordinator init txnInfo)", txnId)
		storage.txnStore[txnId] = NewTxnInfo()
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.txnStore[txnId].receiveFromCoordinator = true

		storage.replicateCommitResult(txnId, make([]*rpc.KeyValue, 0), COORDINATOR_ABORT)
	}
}

func (a Abort2PL) abortProcessedTxn(storage *Storage) {
	txnId := a.abortRequest.TxnId
	if storage.txnStore[txnId].status.IsPrepare() {
		logrus.Debugf("ABORT: %v (coordinator) PREPARED", txnId)
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.replicateCommitResult(txnId, make([]*rpc.KeyValue, 0), COORDINATOR_ABORT)
		storage.releaseKeyAndCheckPrepare(txnId)
		return
	}

	switch storage.txnStore[txnId].status {
	case WAITING:
		logrus.Debugf("ABORT: %v (coordinator) INIT", txnId)
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.setReadResult(storage.txnStore[txnId].readAndPrepareRequestOp, -1, false)
		storage.replicateCommitResult(txnId, make([]*rpc.KeyValue, 0), COORDINATOR_ABORT)
		storage.releaseKeyAndCheckPrepare(txnId)
		break
	default:
		logrus.Fatalf("txn %v should be in statue prepared or init, but status is %v",
			txnId, storage.txnStore[txnId].status)
		break
	}
}
