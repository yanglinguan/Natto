package server

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type ApplyCommitResultGTS struct {
	msg ReplicationMsg
}

func NewApplyCommitResultGTS(msg ReplicationMsg) *ApplyCommitResultGTS {
	return &ApplyCommitResultGTS{msg: msg}
}

func (a ApplyCommitResultGTS) Execute(storage *Storage) {
	log.Debugf("txn %v apply commit result %v", a.msg.TxnId, a.msg.Status)
	if storage.server.IsLeader() {
		return
	}
	storage.initTxnIfNotExist(a.msg)
	if !storage.server.config.GetFastPath() {
		log.Debugf("txn %v apply commit result disable fast path", a.msg.TxnId)
		storage.txnStore[a.msg.TxnId].status = a.msg.Status
		if a.msg.Status == COMMIT {
			storage.txnStore[a.msg.TxnId].commitOrder = storage.committed
			storage.committed++
			storage.txnStore[a.msg.TxnId].commitTime = time.Now()
			storage.writeToDB(a.msg.WriteData)
			storage.print()
		}
		return
	}

	a.fastPathExecute(storage)
}

func (a ApplyCommitResultGTS) fastPathExecute(storage *Storage) {
	log.Debugf("txn %v apply replicated commit result enable fast path, status %v, current status %v",
		a.msg.TxnId, a.msg.Status, storage.txnStore[a.msg.TxnId].status)
	storage.txnStore[a.msg.TxnId].receiveFromCoordinator = true
	storage.txnStore[a.msg.TxnId].isFastPrepare = a.msg.IsFastPathSuccess
	switch storage.txnStore[a.msg.TxnId].status {
	case PREPARED, CONDITIONAL_PREPARED:
		storage.releaseKeyAndCheckPrepare(a.msg.TxnId)
		break
	case WAITING:
		op, ok := storage.txnStore[a.msg.TxnId].readAndPrepareRequestOp.(*ReadAndPrepareGTS)
		if !ok {
			log.Fatalf("txn %v op should be readAndPrepareGTS", a.msg.TxnId)
		}
		storage.kvStore.RemoveFromWaitingList(op)
		storage.setReadResult(storage.txnStore[a.msg.TxnId].readAndPrepareRequestOp, -1, false)
		break
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
