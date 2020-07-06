package server

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type ApplyCommitResultOCC struct {
	msg ReplicationMsg
}

func NewApplyCommitResultOCC(msg ReplicationMsg) *ApplyCommitResultOCC {
	return &ApplyCommitResultOCC{msg: msg}
}

func (a ApplyCommitResultOCC) Execute(storage *Storage) {
	log.Debugf("txn %v apply commit result, status %v current status %v", a.msg.TxnId, a.msg.Status, storage.txnStore[a.msg.TxnId].status)
	storage.txnStore[a.msg.TxnId].receiveFromCoordinator = true
	if storage.txnStore[a.msg.TxnId].status == PREPARED {
		storage.kvStore.ReleaseKeys(storage.txnStore[a.msg.TxnId].readAndPrepareRequestOp)
	}
	storage.txnStore[a.msg.TxnId].status = a.msg.Status
	if a.msg.Status == COMMIT {
		storage.txnStore[a.msg.TxnId].commitOrder = storage.committed
		storage.committed++
		storage.txnStore[a.msg.TxnId].commitTime = time.Now()
		storage.writeToDB(a.msg.WriteData)
		storage.print()
	}
}
