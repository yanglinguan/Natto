package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type RePrepareRequest struct {
	request *rpc.RePrepareRequest
}

func NewRePrepareRequest(request *rpc.RePrepareRequest) *RePrepareRequest {
	return &RePrepareRequest{request: request}
}

func (r RePrepareRequest) Execute(storage *Storage) {
	logrus.Debugf("re-prepare txn %v requested by txn %v counter %v",
		r.request.TxnId, r.request.RequestTxnId, r.request.Counter)
	if r.request.Counter < storage.txnStore[r.request.TxnId].prepareCounter {
		logrus.Debugf("re-prepare request counter %v < txn %v counter %v",
			r.request.Counter, r.request.TxnId,
			storage.txnStore[r.request.TxnId].prepareCounter)
		return
	} else if r.request.Counter > storage.txnStore[r.request.TxnId].prepareCounter {
		logrus.Fatalf("re-prepare request counter %v > txn %v counter %v",
			r.request.Counter, r.request.TxnId,
			storage.txnStore[r.request.TxnId].prepareCounter)
		return
	}

	// add back to the queue
	op, ok := storage.txnStore[r.request.TxnId].readAndPrepareRequestOp.(*ReadAndPrepareHighPriority)
	if !ok {
		logrus.Fatalf("txn %v should convert to gts read and prepare", r.request.TxnId)
	}
	logrus.Debugf("EXECUTE re-prepare: txn %v add back to waiting queue", r.request.TxnId)
	storage.kvStore.AddToWaitingList(op)

	storage.releaseKeyAndCheckPrepare(op.txnId)

}
