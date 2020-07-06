package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
	"time"
)

type CommitGTS struct {
	request *rpc.CommitRequest
}

func NewCommitGTS(request *rpc.CommitRequest) *CommitGTS {
	return &CommitGTS{request: request}
}

func (c *CommitGTS) Execute(storage *Storage) {
	txnId := c.request.TxnId
	log.Infof("COMMITTED: %v", txnId)
	if txnInfo, exist := storage.txnStore[txnId]; !exist || (txnInfo.status != PREPARED && txnInfo.status != CONDITIONAL_PREPARED) {
		log.WithFields(log.Fields{
			"txnId":  txnId,
			"status": txnInfo.status,
		}).Fatal("txn should be prepared before commit")
	}

	storage.txnStore[txnId].status = COMMIT
	storage.txnStore[txnId].isFastPrepare = c.request.IsFastPathSuccess

	storage.replicateCommitResult(txnId, c.request.WriteKeyValList)

	storage.txnStore[txnId].commitTime = time.Now()

	storage.releaseKeyAndCheckPrepare(txnId)

	storage.writeToDB(c.request.WriteKeyValList)
	storage.txnStore[txnId].receiveFromCoordinator = true
	storage.txnStore[txnId].commitOrder = storage.committed

	storage.committed++
	storage.print()
}
