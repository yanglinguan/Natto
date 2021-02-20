package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type AbortOCC struct {
	abortRequest *rpc.AbortRequest
}

func NewAbortOCC(abortRequest *rpc.AbortRequest) *AbortOCC {
	a := &AbortOCC{
		abortRequest: abortRequest,
	}
	return a
}

func (a AbortOCC) Execute(storage *Storage) {
	txnId := a.abortRequest.TxnId
	if txnInfo, exist := storage.txnStore[txnId]; exist {
		txnInfo.receiveFromCoordinator = true
		if txnInfo.status.IsAbort() {
			log.Debugf("txn %v is already abort it self abort reason %v", txnId, txnInfo.status.String())
			return
		}
		switch txnInfo.status {
		case COMMIT:
			log.Fatalf("Error: txn %v is already committed", txnId)
			break
		default:
			log.Debugf("call abort processed txn %v", txnId)
			if storage.server.config.ReadBeforeCommitReplicate() {
				a.abortProcessedTxn(storage)
			} else {
				storage.replicateCommitResult(txnId, make([]*rpc.KeyValue, 0), COORDINATOR_ABORT)
			}
			break
		}
	} else {
		log.Infof("ABORT %v (coordinator init txnInfo)", txnId)
		storage.txnStore[txnId] = NewTxnInfo()
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.txnStore[txnId].receiveFromCoordinator = true

		storage.replicateCommitResult(txnId, make([]*rpc.KeyValue, 0), COORDINATOR_ABORT)
	}
}

func (a AbortOCC) abortProcessedTxn(storage *Storage) {
	txnId := a.abortRequest.TxnId
	log.Debugf("occ store abort processed txn %v, status %v", txnId, storage.txnStore[txnId].status)
	switch storage.txnStore[txnId].status {
	case PREPARED:
		log.Infof("ABORT %v (coordinator) PREPARED", txnId)
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.replicateCommitResult(txnId, make([]*rpc.KeyValue, 0), COORDINATOR_ABORT)
		storage.kvStore.ReleaseKeys(storage.txnStore[txnId].readAndPrepareRequestOp)
		break
	default:
		log.Fatalf("txn %v should be in statue prepared, but status is %v",
			txnId, storage.txnStore[txnId].status)
		break
	}
}
