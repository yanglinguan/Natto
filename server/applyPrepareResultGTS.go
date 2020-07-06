package server

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type GTSApplyPrepareReplicationMsg struct {
	msg ReplicationMsg
}

func NewGTSApplyPrepareReplicationMsg(msg ReplicationMsg) *GTSApplyPrepareReplicationMsg {
	r := &GTSApplyPrepareReplicationMsg{msg: msg}
	return r
}

func (p *GTSApplyPrepareReplicationMsg) Execute(storage *Storage) {
	log.Debugf("txn %v apply prepare result %v", p.msg.TxnId, p.msg.Status)
	if storage.server.IsLeader() {
		storage.sendPrepareResult(p.msg.TxnId)
		if p.msg.Status == REVERSE_REORDER_PREPARED {
			storage.sendReverseReorderRequest(p.msg.TxnId)
		}
		return
	}

	storage.initTxnIfNotExist(p.msg)
	if !storage.server.config.GetFastPath() {
		storage.txnStore[p.msg.TxnId].status = p.msg.Status
		return
	}

	p.fastPathExecution(storage)
}

func (p *GTSApplyPrepareReplicationMsg) fastPathExecution(storage *Storage) {
	if storage.txnStore[p.msg.TxnId].receiveFromCoordinator {
		log.Debugf("txn %v already receive the result from coordinator", p.msg.TxnId)
		// already receive final decision from coordinator
		return
	}

	log.Debugf("txn %v fast path status %v, slow path status %v", p.msg.TxnId, storage.txnStore[p.msg.TxnId].status, p.msg.Status)
	switch storage.txnStore[p.msg.TxnId].status {
	case PREPARED, CONDITIONAL_PREPARED, REORDER_PREPARED:
		storage.txnStore[p.msg.TxnId].status = p.msg.Status
		if p.msg.Status == ABORT {
			log.Debugf("CONFLICT: txn %v fast path prepare but slow path abort, abort", p.msg.TxnId)
			storage.releaseKeyAndCheckPrepare(p.msg.TxnId)
		}
		break
	case ABORT:
		storage.txnStore[p.msg.TxnId].status = p.msg.Status
		if p.msg.Status == PREPARED || p.msg.Status == CONDITIONAL_PREPARED || p.msg.Status == REORDER_PREPARED {
			storage.txnStore[p.msg.TxnId].preparedTime = time.Now()
			log.Debugf("CONFLICT: txn %v fast path abort but slow path prepare, prepare", p.msg.TxnId)
			storage.kvStore.RecordPrepared(storage.txnStore[p.msg.TxnId].readAndPrepareRequestOp)
		}
		break
	case WAITING:
		log.Debugf("txn %v fast path waiting the lock slow path status %v", p.msg.TxnId, p.msg.Status)
		storage.txnStore[p.msg.TxnId].status = p.msg.Status
		op, ok := storage.txnStore[p.msg.TxnId].readAndPrepareRequestOp.(*ReadAndPrepareGTS)
		if !ok {
			log.Fatalf("txn %v read and prepare should gts", op.txnId)
		}
		storage.kvStore.RemoveFromWaitingList(op)
		storage.setReadResult(storage.txnStore[p.msg.TxnId].readAndPrepareRequestOp, -1, false)
		if p.msg.Status == PREPARED || p.msg.Status == CONDITIONAL_PREPARED || p.msg.Status == REORDER_PREPARED {
			storage.txnStore[p.msg.TxnId].preparedTime = time.Now()
			storage.kvStore.RecordPrepared(storage.txnStore[p.msg.TxnId].readAndPrepareRequestOp)
		}
		break
	case INIT:
		log.Debugf("txn %v fast path not stated slow path status %v ", p.msg.TxnId, p.msg.Status)
		storage.txnStore[p.msg.TxnId].status = p.msg.Status
		if p.msg.Status == PREPARED || p.msg.Status == CONDITIONAL_PREPARED || p.msg.Status == REORDER_PREPARED {
			storage.kvStore.RecordPrepared(storage.txnStore[p.msg.TxnId].readAndPrepareRequestOp)
		}
		break
	}
}
