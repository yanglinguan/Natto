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
		log.Debugf("txn %v already have finial decision %v", storage.txnStore[o.msg.TxnId].status.String())
		// already receive final decision from coordinator
		return
	}

	currentStatus := storage.txnStore[o.msg.TxnId].status
	log.Debugf("txn %v fast path status %v, slow path status %v",
		o.msg.TxnId, currentStatus.String(), o.msg.Status.String())
	storage.txnStore[o.msg.TxnId].status = o.msg.Status
	if currentStatus.IsPrepare() {
		if o.msg.Status.IsAbort() {
			log.Debugf("CONFLICT: txn %v fast path prepared but slow path abort", o.msg.TxnId)
			storage.kvStore.ReleaseKeys(storage.txnStore[o.msg.TxnId].readAndPrepareRequestOp)
		}
	} else if currentStatus.IsAbort() {
		if o.msg.Status.IsPrepare() {
			log.Debugf("CONFLICT: txn %v fast path abort but slow path prepared", o.msg.TxnId)
			storage.kvStore.RecordPrepared(storage.txnStore[o.msg.TxnId].readAndPrepareRequestOp)
		}
	} else if currentStatus == INIT {
		log.Debugf("txn %v does not start fast path yet, slow path status %v",
			o.msg.TxnId, o.msg.Status.String())
		if o.msg.Status == PREPARED {
			storage.kvStore.RecordPrepared(storage.txnStore[o.msg.TxnId].readAndPrepareRequestOp)
		}
	}
}
