package server

import (
	log "github.com/sirupsen/logrus"
)

type ApplyCommitResult2PL struct {
	msg *ReplicationMsg
}

func NewApplyCommitResult2PL(msg *ReplicationMsg) *ApplyCommitResult2PL {
	return &ApplyCommitResult2PL{msg: msg}
}

func (a ApplyCommitResult2PL) Execute(storage *Storage) {
	txnId := a.msg.TxnId
	log.Debugf("txn %v apply commit result %v", a.msg.TxnId, a.msg.Status)
	if storage.server.IsLeader() {
		if !storage.server.config.ReadBeforeCommitReplicate() {
			if a.msg.Status == COMMIT {
				storage.commit(txnId, COMMIT, a.msg.WriteData)
				storage.releaseKeyAndCheckPrepare(txnId)
			} else {
				a.abortProcessedTxn(storage)
			}

		}
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

func (a ApplyCommitResult2PL) fastPathExecute(storage *Storage) {
	log.Debugf("txn %v apply replicated commit result enable fast path, status %v, current status %v",
		a.msg.TxnId, a.msg.Status, storage.txnStore[a.msg.TxnId].status.String())
	storage.txnStore[a.msg.TxnId].receiveFromCoordinator = true
	storage.txnStore[a.msg.TxnId].isFastPrepare = a.msg.IsFastPathSuccess
	if storage.txnStore[a.msg.TxnId].status.IsPrepare() {
		storage.releaseKeyAndCheckPrepare(a.msg.TxnId)
	} else if storage.txnStore[a.msg.TxnId].status == WAITING {
		op, ok := storage.txnStore[a.msg.TxnId].readAndPrepareRequestOp.(LockingOp)
		if !ok {
			log.Fatalf("txn %v op should be readAndPrepareGTS", a.msg.TxnId)
		}
		storage.kvStore.RemoveFromWaitingList(op)
		storage.setReadResult(storage.txnStore[a.msg.TxnId].readAndPrepareRequestOp, -1, false)
	}

	storage.commit(a.msg.TxnId, a.msg.Status, a.msg.WriteData)
}

func (a ApplyCommitResult2PL) abortProcessedTxn(storage *Storage) {
	txnId := a.msg.TxnId
	if storage.txnStore[txnId].status.IsPrepare() {
		log.Debugf("ABORT: %v (coordinator) PREPARED", txnId)
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.releaseKeyAndCheckPrepare(txnId)
		return
	}

	switch storage.txnStore[txnId].status {
	case WAITING:
		log.Debugf("ABORT: %v (coordinator) INIT", txnId)
		storage.setReadResult(storage.txnStore[txnId].readAndPrepareRequestOp, -1, false)
		storage.txnStore[txnId].status = COORDINATOR_ABORT
		storage.releaseKeyAndCheckPrepare(txnId)
		break
	}
}
