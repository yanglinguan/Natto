package server

import (
	log "github.com/sirupsen/logrus"
)

type ApplyCommitResultTO struct {
	msg *ReplicationMsg
}

func NewApplyCommitResultTO(msg *ReplicationMsg) *ApplyCommitResultTO {
	return &ApplyCommitResultTO{msg: msg}
}

func (a ApplyCommitResultTO) Execute(storage *Storage) {
	txnId := a.msg.TxnId
	log.Debugf("txn %v apply commit result %v", txnId, a.msg.Status.String())
	if storage.server.IsLeader() {
		if !storage.server.config.ReadBeforeCommitReplicate() {
			storage.commitTO(txnId, a.msg.Status, a.msg.WriteData, a.msg.ReadTS, a.msg.WriteTS)
			storage.kvStore.ReleaseKeys(storage.txnStore[txnId].readAndPrepareRequestOp)
		}
		return
	}
	storage.initTxnIfNotExist(a.msg)
	if !storage.server.config.GetFastPath() {
		log.Debugf("txn %v apply commit result disable fast path", a.msg.TxnId)
		storage.commitTO(txnId, a.msg.Status, a.msg.WriteData, a.msg.ReadTS, a.msg.WriteTS)
		return
	}

	a.fastPathExecute(storage)
}

func (a ApplyCommitResultTO) fastPathExecute(storage *Storage) {
	log.Debugf("txn %v apply commit result, status %v current status %v",
		a.msg.TxnId, a.msg.Status, storage.txnStore[a.msg.TxnId].status.String())
	storage.txnStore[a.msg.TxnId].receiveFromCoordinator = true
	if storage.txnStore[a.msg.TxnId].status == PREPARED {
		storage.kvStore.ReleaseKeys(storage.txnStore[a.msg.TxnId].readAndPrepareRequestOp)
	}
	storage.commitTO(a.msg.TxnId, a.msg.Status, a.msg.WriteData, a.msg.ReadTS, a.msg.WriteTS)
}
