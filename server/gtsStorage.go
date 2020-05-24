package server

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type GTSStorage struct {
	*AbstractStorage
}

func NewGTSStorage(server *Server) *GTSStorage {
	s := &GTSStorage{
		NewAbstractStorage(server),
	}

	s.AbstractStorage.abstractMethod = s

	return s
}

func (s *GTSStorage) Commit(op *CommitRequestOp) {
	txnId := op.request.TxnId
	log.Infof("COMMITTED: %v", txnId)
	if txnInfo, exist := s.txnStore[txnId]; !exist || (txnInfo.status != PREPARED && txnInfo.status != CONDITIONAL_PREPARED) {
		log.WithFields(log.Fields{
			"txnId":  txnId,
			"status": txnInfo.status,
		}).Fatal("txn should be prepared before commit")
	}

	op.wait <- true

	s.txnStore[txnId].status = COMMIT
	s.txnStore[txnId].isFastPrepare = op.request.IsFastPathSuccess

	s.replicateCommitResult(txnId, op.request.WriteKeyValList)

	s.txnStore[txnId].commitTime = time.Now()

	s.releaseKeyAndCheckPrepare(txnId)

	s.writeToDB(op.request.WriteKeyValList)
	s.txnStore[txnId].receiveFromCoordinator = true
	s.txnStore[txnId].commitOrder = s.committed

	s.committed++
	s.print()
}

func (s *GTSStorage) abortProcessedTxn(txnId string) {
	switch s.txnStore[txnId].status {
	case PREPARED, CONDITIONAL_PREPARED:
		log.Infof("ABORT: %v (coordinator) PREPARED", txnId)
		s.txnStore[txnId].status = ABORT
		s.replicateCommitResult(txnId, nil)
		s.releaseKeyAndCheckPrepare(txnId)
		break
	case WAITING:
		log.Infof("ABORT: %v (coordinator) INIT", txnId)
		s.txnStore[txnId].status = ABORT
		s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
		s.replicateCommitResult(txnId, nil)
		s.releaseKeyAndCheckPrepare(txnId)
		break
	default:
		log.Fatalf("txn %v should be in statue prepared or init, but status is %v",
			txnId, s.txnStore[txnId].status)
		break
	}
}

func (s *GTSStorage) checkKeysAvailableForHighPriorityTxn(op *ReadAndPrepareOp) (bool, map[int]bool) {
	// first check if there is high priority txn in front holding the same keys
	// readKeyMap store the keys in this partition
	// read-write conflict

	for rk := range op.readKeyMap {
		if len(s.kvStore[rk].PreparedTxnWrite) > 0 {
			// there is high priority txn before it
			log.Debugf("txn %v (read) : there is txn (write) a high priority txn holding key %v",
				op.txnId, rk)
			return false, make(map[int]bool)
		}
	}

	// write-read, write-write conflict
	for wk := range op.writeKeyMap {
		if len(s.kvStore[wk].PreparedTxnWrite) > 0 || len(s.kvStore[wk].PreparedTxnRead) > 0 {
			log.Debugf("txn %v (write) : there is txn a high priority txn holding key %v",
				op.txnId, wk)
			return false, make(map[int]bool)
		}
	}

	// if txn does not has conflict with other high priority txn in this partition
	// check if there is a conflict with low priority txn in the other partition
	// find out the conditions to prepare

	overlapPartition := s.findOverlapPartitionsWithLowPriorityTxn(op)

	log.Debugf("txn %v keys are available condition %v", op.txnId, overlapPartition)

	return true, overlapPartition
}

func (s *GTSStorage) prepared(op *ReadAndPrepareOp, condition map[int]bool) {
	log.Debugf("PREPARED txn %v priority %v condition %v", op.txnId, op.request.Txn.HighPriority, condition)
	s.removeFromQueue(op)
	// record the prepared keys
	txnId := op.txnId
	if len(condition) > 0 {
		log.Debugf("txn %v conditional prepare %v", op.txnId, condition)
		s.txnStore[txnId].status = CONDITIONAL_PREPARED
	} else {
		s.txnStore[txnId].status = PREPARED
	}
	s.setReadResult(op)
	// with read-only optimization, we do not need to record the prepared
	// and do not need to replicate
	if s.server.config.GetIsReadOnly() && op.request.Txn.ReadOnly {
		if s.txnStore[txnId].inQueue {
			s.server.executor.ReleaseReadOnlyTxn <- op
		}
		s.setPrepareResult(op, condition)
		if s.server.config.GetPriority() {
			s.readyToSendPrepareResultToCoordinator(s.txnStore[txnId].prepareResultOp)
		}
	} else {
		s.recordPrepared(op)
		s.setPrepareResult(op, condition)
		s.replicatePreparedResult(op.txnId)
	}
}

func (s *GTSStorage) Prepare(op *ReadAndPrepareOp) {
	log.Infof("PROCESSING txn %v priority %v", op.txnId, op.request.Txn.HighPriority)
	txnId := op.txnId
	if info, exist := s.txnStore[txnId]; exist && info.status != INIT {
		log.Infof("txn %v is already has status %v", txnId, s.txnStore[txnId].status)
		s.setReadResult(op)
		return
	}

	s.txnStore[txnId] = &TxnInfo{
		readAndPrepareRequestOp: op,
		status:                  INIT,
		receiveFromCoordinator:  false,
		waitingTxnKey:           0,
		waitingTxnDep:           0,
		startTime:               time.Now(),
	}
	s.txnStore[txnId].startTime = time.Now()

	if op.selfAbort {
		s.txnStore[txnId].status = ABORT
		s.txnStore[txnId].selfAbort = true
		log.Warnf("txn %v self aborted", txnId)
		//	s.setReadResult(op)
		s.selfAbort(op)
		return
	}

	hasWaiting := s.hasWaitingTxn(op)
	canPrepare := !hasWaiting
	condition := make(map[int]bool)
	if op.request.Txn.HighPriority {
		if !hasWaiting {
			canPrepare, condition = s.checkKeysAvailableForHighPriorityTxn(op)
		}
		_, exist := condition[s.server.partitionId]
		// if txn only conflict with low priority txn on this partition, then wait
		if canPrepare && (len(condition) != 1 || !exist) {
			// prepare
			s.prepared(op, condition)
		} else {
			// wait
			if !op.passedTimestamp {
				s.txnStore[txnId].status = WAITING
				log.Debugf("txn %v cannot prepare available wait", txnId)
				s.addToQueue(op.keyMap, op)
			} else {
				s.txnStore[txnId].status = ABORT
				log.Debugf("txn %v passed timestamp also cannot prepared", txnId)
				//	s.setReadResult(op)
				s.selfAbort(op)
			}
		}

	} else {
		if !op.request.Txn.ReadOnly || !s.server.config.GetIsReadOnly() {
			s.setReadResult(op)
		}

		if !hasWaiting {
			canPrepare = s.checkKeysAvailableForLowPriorityTxn(op)
		}
		if canPrepare {
			// prepare
			s.prepared(op, condition)
		} else {
			// abort
			log.Debugf("txn %v low priority abort", txnId)
			s.txnStore[txnId].status = ABORT
			s.selfAbort(op)
		}

	}
}

func (s *GTSStorage) applyReplicatedPrepareResult(msg ReplicationMsg) {
	if s.txnStore[msg.TxnId].receiveFromCoordinator {
		log.Debugf("txn %v already receive the result from coordinator", msg.TxnId)
		// already receive final decision from coordinator
		return
	}
	log.Debugf("txn %v fast path status %v, slow path status %v", msg.TxnId, s.txnStore[msg.TxnId].status, msg.Status)
	switch s.txnStore[msg.TxnId].status {
	case PREPARED, CONDITIONAL_PREPARED:
		s.txnStore[msg.TxnId].status = msg.Status
		if msg.Status == ABORT {
			log.Debugf("CONFLICT: txn %v fast path prepare but slow path abort, abort", msg.TxnId)
			s.releaseKeyAndCheckPrepare(msg.TxnId)
		}
		break
	case ABORT:
		s.txnStore[msg.TxnId].status = msg.Status
		if msg.Status == PREPARED {
			s.txnStore[msg.TxnId].preparedTime = time.Now()
			log.Debugf("CONFLICT: txn %v fast path abort but slow path prepare, prepare", msg.TxnId)
			s.recordPrepared(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		}
		break
	case WAITING:
		log.Debugf("txn %v fast path waiting the lock slow path status %v", msg.TxnId, msg.Status)
		s.txnStore[msg.TxnId].status = msg.Status
		s.removeFromQueue(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		s.setReadResult(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		if msg.Status == PREPARED {
			s.txnStore[msg.TxnId].preparedTime = time.Now()
			s.recordPrepared(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		}
		break
	case INIT:
		log.Debugf("txn %v fast path not stated slow path status %v ", msg.TxnId, msg.Status)
		s.txnStore[msg.TxnId].status = msg.Status
		if msg.Status == PREPARED {
			s.recordPrepared(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		}
		break
	}
}

func (s *GTSStorage) applyReplicatedCommitResult(msg ReplicationMsg) {
	log.Debugf("txn %v apply replicated commit result enable fast path, status %v, current status %v",
		msg.TxnId, msg.Status, s.txnStore[msg.TxnId].status)
	s.txnStore[msg.TxnId].receiveFromCoordinator = true
	s.txnStore[msg.TxnId].isFastPrepare = msg.IsFastPathSuccess
	switch s.txnStore[msg.TxnId].status {
	case PREPARED, CONDITIONAL_PREPARED:
		s.releaseKeyAndCheckPrepare(msg.TxnId)
		break
	case WAITING:
		s.removeFromQueue(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		s.setReadResult(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		break
	}
	s.txnStore[msg.TxnId].status = msg.Status
	if msg.Status == COMMIT {
		s.txnStore[msg.TxnId].commitOrder = s.committed
		s.committed++
		s.txnStore[msg.TxnId].commitTime = time.Now()
		s.writeToDB(msg.WriteData)
		s.print()
	}
}
