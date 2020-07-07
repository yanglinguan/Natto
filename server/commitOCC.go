package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type CommitOCC struct {
	request *rpc.CommitRequest
}

func NewCommitOCC(request *rpc.CommitRequest) *CommitOCC {
	return &CommitOCC{request: request}
}

func (c *CommitOCC) Execute(storage *Storage) {
	txnId := c.request.TxnId
	log.Infof("COMMIT %v", txnId)
	storage.commit(txnId, COMMIT, c.request.WriteKeyValList)
	//storage.txnStore[txnId].status = COMMIT
	//storage.txnStore[txnId].isFastPrepare = c.request.IsFastPathSuccess
	//storage.writeToDB(c.request.WriteKeyValList)
	storage.replicateCommitResult(txnId, c.request.WriteKeyValList)
	storage.kvStore.ReleaseKeys(storage.txnStore[txnId].readAndPrepareRequestOp)

	//storage.txnStore[txnId].receiveFromCoordinator = true
	//storage.txnStore[txnId].commitOrder = storage.committed
	//storage.committed++

	//storage.print()
}
