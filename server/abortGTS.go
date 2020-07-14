package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type AbortGTS struct {
	abortRequest *rpc.AbortRequest
}

func NewAbortGTS(abortRequest *rpc.AbortRequest) *AbortGTS {
	a := &AbortGTS{
		abortRequest: abortRequest,
	}
	return a
}

func (a AbortGTS) Execute(storage *Storage) {
	txnId := a.abortRequest.TxnId
	if txnInfo, exist := storage.txnStore[txnId]; exist {
		txnInfo.receiveFromCoordinator = true
		if txnInfo.status.IsAbort() {
			log.Debugf("txn %v is already abort it self %v abort reason", txnId, txnInfo.status.String())
			return
		}
		switch txnInfo.status {
		case COMMIT:
			log.Fatalf("Error: txn %v is already committed", txnId)
			break
		default:
			log.Debugf("call abort processed txn %v", txnId)
			a.abortProcessedTxn(storage)
			break
		}
	} else {
		log.Infof("ABORT %v (coordinator init txnInfo)", txnId)
		storage.txnStore[txnId] = NewTxnInfo()
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.txnStore[txnId].receiveFromCoordinator = true

		storage.replicateCommitResult(txnId, make([]*rpc.KeyValue, 0))
	}
}

func (a AbortGTS) abortProcessedTxn(storage *Storage) {
	txnId := a.abortRequest.TxnId
	if storage.txnStore[txnId].status.IsPrepare() {
		log.Debugf("ABORT: %v (coordinator) PREPARED", txnId)
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.replicateCommitResult(txnId, make([]*rpc.KeyValue, 0))
		storage.releaseKeyAndCheckPrepare(txnId)
		return
	}

	switch storage.txnStore[txnId].status {
	case WAITING:
		log.Infof("ABORT: %v (coordinator) INIT", txnId)
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.setReadResult(storage.txnStore[txnId].readAndPrepareRequestOp, -1, false)
		storage.replicateCommitResult(txnId, make([]*rpc.KeyValue, 0))
		storage.releaseKeyAndCheckPrepare(txnId)
		break
	default:
		log.Fatalf("txn %v should be in statue prepared or init, but status is %v",
			txnId, storage.txnStore[txnId].status)
		break
	}
}
