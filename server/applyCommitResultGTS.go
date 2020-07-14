package server

import (
	log "github.com/sirupsen/logrus"
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
		storage.commit(a.msg.TxnId, a.msg.Status, a.msg.WriteData)
		return
	}

	a.fastPathExecute(storage)
}

func (a ApplyCommitResultGTS) fastPathExecute(storage *Storage) {
	log.Debugf("txn %v apply replicated commit result enable fast path, status %v, current status %v",
		a.msg.TxnId, a.msg.Status, storage.txnStore[a.msg.TxnId].status.String())
	storage.txnStore[a.msg.TxnId].receiveFromCoordinator = true
	storage.txnStore[a.msg.TxnId].isFastPrepare = a.msg.IsFastPathSuccess
	if storage.txnStore[a.msg.TxnId].status.IsPrepare() {
		storage.releaseKeyAndCheckPrepare(a.msg.TxnId)
	} else if storage.txnStore[a.msg.TxnId].status == WAITING {
		op, ok := storage.txnStore[a.msg.TxnId].readAndPrepareRequestOp.(*ReadAndPrepareGTS)
		if !ok {
			log.Fatalf("txn %v op should be readAndPrepareGTS", a.msg.TxnId)
		}
		storage.kvStore.RemoveFromWaitingList(op)
		storage.setReadResult(storage.txnStore[a.msg.TxnId].readAndPrepareRequestOp, -1, false)
	}

	storage.commit(a.msg.TxnId, a.msg.Status, a.msg.WriteData)
}
