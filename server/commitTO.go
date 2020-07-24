package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type CommitTO struct {
	request *rpc.CommitRequest
}

func NewCommitTO(request *rpc.CommitRequest) *CommitTO {
	return &CommitTO{request: request}
}

func (c *CommitTO) Execute(storage *Storage) {
	txnId := c.request.TxnId
	log.Infof("COMMIT %v", txnId)
	storage.commitTO(txnId, COMMIT, c.request.WriteKeyValList,
		storage.txnStore[txnId].readAndPrepareRequestOp.GetTimestamp(),
		storage.txnStore[txnId].readAndPrepareRequestOp.GetTimestamp())
	storage.replicateCommitResult(txnId, c.request.WriteKeyValList)
	storage.kvStore.ReleaseKeys(storage.txnStore[txnId].readAndPrepareRequestOp)
}