package server

import (
	"Carousel-GTS/rpc"
)

type CommitGTS struct {
	*Commit2PL
	//request *rpc.CommitRequest
}

func NewCommitGTS(request *rpc.CommitRequest) *CommitGTS {
	return &CommitGTS{NewCommit2PL(request)}
}

//func (c *CommitGTS) Execute(storage *Storage) {
//	txnId := c.request.TxnId
//	log.Infof("COMMITTED: %v", txnId)
//	if txnInfo, exist := storage.txnStore[txnId]; !exist {
//		log.Fatalf("txn %v status %v should be prepared before commit", txnId, txnInfo.status.String())
//	}
//
//	storage.commit(txnId, COMMIT, c.request.WriteKeyValList)
//	storage.replicateCommitResult(txnId, c.request.WriteKeyValList)
//	storage.releaseKeyAndCheckPrepare(txnId)
//}
