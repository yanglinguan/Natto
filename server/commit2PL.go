package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type Commit2PL struct {
	request *rpc.CommitRequest
}

func NewCommit2PL(request *rpc.CommitRequest) *Commit2PL {
	return &Commit2PL{request: request}
}

func (c *Commit2PL) Execute(storage *Storage) {
	txnId := c.request.TxnId
	log.Infof("COMMITTED: %v", txnId)
	if txnInfo, exist := storage.txnStore[txnId]; !exist {
		log.Fatalf("txn %v status %v should be prepared before commit", txnId, txnInfo.status.String())
	}

	storage.commit(txnId, COMMIT, c.request.WriteKeyValList)
	storage.replicateCommitResult(txnId, c.request.WriteKeyValList)
	storage.releaseKeyAndCheckPrepare(txnId)
}