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

	storage.txnStore[txnId].status = COMMIT
	storage.replicateCommitResult(txnId, c.request.WriteKeyValList)

	if storage.server.config.ReadBeforeCommitReplicate() {
		storage.commit(txnId, COMMIT, c.request.WriteKeyValList)
		storage.kvStore.ReleaseKeys(storage.txnStore[txnId].readAndPrepareRequestOp)
	}
}
