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
	txnId := a.msg.TxnId
	log.Debugf("txn %v apply commit result %v", txnId, a.msg.Status.String())
	if storage.server.IsLeader() {
		if !storage.server.config.ReadBeforeCommitReplicate() {
			storage.commit(txnId, COMMIT, a.msg.WriteData)
			storage.kvStore.ReleaseKeys(storage.txnStore[txnId].readAndPrepareRequestOp)
		}
		return
	}
	storage.initTxnIfNotExist(a.msg)
	if !storage.server.config.GetFastPath() {
		log.Debugf("txn %v apply commit result disable fast path", txnId)
		storage.commit(txnId, a.msg.Status, a.msg.WriteData)
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
