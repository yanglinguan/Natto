package server

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type OccStorage struct {
	*AbstractStorage
}

func NewOccStorage(server *Server) *OccStorage {
	o := &OccStorage{
		NewAbstractStorage(server),
	}

	o.AbstractStorage.abstractMethod = o

	return o
}

func (s *OccStorage) prepared(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	s.txnStore[txnId].status = PREPARED
	s.recordPrepared(op)
	s.setPrepareResult(op)
	s.replicatePreparedResult(op.request.Txn.TxnId)
}

func (s *OccStorage) Prepare(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	if txnInfo, exist := s.txnStore[txnId]; exist && txnInfo.status != INIT {
		s.setReadResult(op)
		return
	}

	s.txnStore[txnId] = &TxnInfo{
		readAndPrepareRequestOp: op,
		status:                  INIT,
		receiveFromCoordinator:  false,
		commitOrder:             0,
	}

	s.setReadResult(op)

	available := s.checkKeysAvailable(op)

	if available {
		s.prepared(op)
	} else {
		s.txnStore[txnId].status = ABORT
		s.selfAbort(op)
	}
}

func (s *OccStorage) Commit(op *CommitRequestOp) {
	txnId := op.request.TxnId
	log.Infof("COMMIT %v", txnId)
	s.txnStore[txnId].status = COMMIT
	s.txnStore[txnId].isFastPrepare = op.request.IsFastPathSuccess
	s.replicateCommitResult(txnId, op.request.WriteKeyValList)
	s.releaseKey(txnId)
	s.writeToDB(op.request.WriteKeyValList)

	s.txnStore[txnId].receiveFromCoordinator = true
	s.txnStore[txnId].commitOrder = s.committed
	s.committed++
	op.wait <- true
	s.print()
}

func (s *OccStorage) abortProcessedTxn(txnId string) {
	log.Debugf("occ store abort processed txn %v, status %v", txnId, s.txnStore[txnId].status)
	switch s.txnStore[txnId].status {
	case PREPARED:
		log.Infof("ABORT %v (coordinator) PREPARED", txnId)
		s.txnStore[txnId].status = ABORT
		s.replicateCommitResult(txnId, nil)
		s.releaseKey(txnId)
		break
	default:
		log.Fatalf("txn %v should be in statue prepared, but status is %v",
			txnId, s.txnStore[txnId].status)
		break
	}

}

func (s *OccStorage) applyReplicatedPrepareResult(msg ReplicationMsg) {
	log.Debugf("txn %v apply prepared result", msg.TxnId)
	if s.txnStore[msg.TxnId].receiveFromCoordinator {
		log.Debugf("txn %v already have finial decision %v", s.txnStore[msg.TxnId].status)
		// already receive final decision from coordinator
		return
	}
	log.Debugf("txn %v fast path status %v, slow path status %v", msg.TxnId, s.txnStore[msg.TxnId].status, msg.Status)
	switch s.txnStore[msg.TxnId].status {
	case PREPARED:
		if msg.Status == ABORT {
			log.Debugf("CONFLICT: txn %v fast path prepared but slow path abort", msg.TxnId)
			s.releaseKey(msg.TxnId)
			s.txnStore[msg.TxnId].status = msg.Status
		}
		break
	case ABORT:
		if msg.Status == PREPARED {
			log.Debugf("CONFLICT: txn %v fast path abort but slow path prepared", msg.TxnId)
			s.recordPrepared(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
			s.txnStore[msg.TxnId].status = msg.Status
		}
		break
	case INIT:
		log.Debugf("txn %v does not start fast path yet, slow path status %v", msg.TxnId, msg.Status)
		s.txnStore[msg.TxnId].status = msg.Status
		if msg.Status == PREPARED {
			s.recordPrepared(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		}
		break
	}
}

func (s *OccStorage) applyReplicatedCommitResult(msg ReplicationMsg) {
	log.Debugf("txn %v apply commit result, status %v current status %v", msg.TxnId, msg.Status, s.txnStore[msg.TxnId].status)
	s.txnStore[msg.TxnId].receiveFromCoordinator = true
	if s.txnStore[msg.TxnId].status == PREPARED {
		s.releaseKey(msg.TxnId)
	}

	if msg.Status == COMMIT {
		s.txnStore[msg.TxnId].commitOrder = s.committed
		s.committed++
		s.txnStore[msg.TxnId].commitTime = time.Now()
		s.writeToDB(msg.WriteData)
		s.print()
	}
}
