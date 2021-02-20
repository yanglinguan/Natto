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
			if a.msg.Status == COMMIT {
				storage.commitTO(txnId, a.msg.Status, a.msg.WriteData, a.msg.ReadTS, a.msg.WriteTS)
				storage.kvStore.ReleaseKeys(storage.txnStore[txnId].readAndPrepareRequestOp)
			} else {
				a.abortProcessedTxn(storage)
			}
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

func (a ApplyCommitResultTO) abortProcessedTxn(storage *Storage) {
	txnId := a.msg.TxnId
	log.Debugf("occ store abort processed txn %v, status %v", txnId, storage.txnStore[txnId].status)
	switch storage.txnStore[txnId].status {
	case PREPARED:
		log.Infof("ABORT %v (coordinator) PREPARED", txnId)
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.kvStore.ReleaseKeys(storage.txnStore[txnId].readAndPrepareRequestOp)
		break
	}
}
