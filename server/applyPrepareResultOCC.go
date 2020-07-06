package server

import log "github.com/sirupsen/logrus"

type OCCApplyPrepareReplicationMsg struct {
	msg ReplicationMsg
}

func NewOCCApplyPrepareReplicationMsg(msg ReplicationMsg) *OCCApplyPrepareReplicationMsg {
	o := &OCCApplyPrepareReplicationMsg{msg: msg}
	return o
}

func (o *OCCApplyPrepareReplicationMsg) Execute(storage *Storage) {
	log.Debugf("txn %v apply prepare result %v", o.msg.TxnId, o.msg.Status)
	if storage.server.IsLeader() {
		storage.sendPrepareResult(o.msg.TxnId)
		return
	}

	storage.initTxnIfNotExist(o.msg)
	if !storage.server.config.GetFastPath() {
		storage.txnStore[o.msg.TxnId].status = o.msg.Status
		return
	}

	o.fastPathExecution(storage)
}

func (o *OCCApplyPrepareReplicationMsg) fastPathExecution(storage *Storage) {
	log.Debugf("txn %v apply prepared result", o.msg.TxnId)
	if storage.txnStore[o.msg.TxnId].receiveFromCoordinator {
		log.Debugf("txn %v already have finial decision %v", storage.txnStore[o.msg.TxnId].status)
		// already receive final decision from coordinator
		return
	}
	log.Debugf("txn %v fast path status %v, slow path status %v", o.msg.TxnId, storage.txnStore[o.msg.TxnId].status, o.msg.Status)
	switch storage.txnStore[o.msg.TxnId].status {
	case PREPARED:
		storage.txnStore[o.msg.TxnId].status = o.msg.Status
		if o.msg.Status == ABORT {
			log.Debugf("CONFLICT: txn %v fast path prepared but slow path abort", o.msg.TxnId)
			storage.kvStore.ReleaseKeys(storage.txnStore[o.msg.TxnId].readAndPrepareRequestOp)
		}
		break
	case ABORT:
		storage.txnStore[o.msg.TxnId].status = o.msg.Status
		if o.msg.Status == PREPARED {
			log.Debugf("CONFLICT: txn %v fast path abort but slow path prepared", o.msg.TxnId)
			storage.kvStore.RecordPrepared(storage.txnStore[o.msg.TxnId].readAndPrepareRequestOp)
		}
		break
	case INIT:
		log.Debugf("txn %v does not start fast path yet, slow path status %v", o.msg.TxnId, o.msg.Status)
		storage.txnStore[o.msg.TxnId].status = o.msg.Status
		if o.msg.Status == PREPARED {
			storage.kvStore.RecordPrepared(storage.txnStore[o.msg.TxnId].readAndPrepareRequestOp)
		}
		break
	}
}
