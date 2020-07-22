package server

import (
	log "github.com/sirupsen/logrus"
)

type ApplyCommitResultOCC struct {
	msg *ReplicationMsg
}

func NewApplyCommitResultOCC(msg *ReplicationMsg) *ApplyCommitResultOCC {
	return &ApplyCommitResultOCC{msg: msg}
}

func (a ApplyCommitResultOCC) Execute(storage *Storage) {
	log.Debugf("txn %v apply commit result %v", a.msg.TxnId, a.msg.Status.String())
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

func (a ApplyCommitResultOCC) fastPathExecute(storage *Storage) {
	log.Debugf("txn %v apply commit result, status %v current status %v",
		a.msg.TxnId, a.msg.Status, storage.txnStore[a.msg.TxnId].status.String())
	storage.txnStore[a.msg.TxnId].receiveFromCoordinator = true
	if storage.txnStore[a.msg.TxnId].status == PREPARED {
		storage.kvStore.ReleaseKeys(storage.txnStore[a.msg.TxnId].readAndPrepareRequestOp)
	}
	storage.commit(a.msg.TxnId, a.msg.Status, a.msg.WriteData)
}
