package server

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type GTSStorageWithReorder struct {
	*AbstractStorage

	graph *Graph
}

func NewGTSStorageWithReorder(server *Server) *GTSStorageWithReorder {
	s := &GTSStorageWithReorder{
		AbstractStorage: NewAbstractStorage(server),
		graph:           NewDependencyGraph(),
	}

	s.AbstractStorage.abstractMethod = s

	return s
}

func (s GTSStorageWithReorder) hasConflictOnOtherPartition(txnId string, conflictTxnId string) bool {
	x := s.txnStore[txnId].readAndPrepareRequestOp.partitionKeys
	y := s.txnStore[conflictTxnId].readAndPrepareRequestOp.partitionKeys

	for pId, keys := range x {
		if pId == s.server.partitionId {
			continue
		}
		for key := range keys {
			if _, exist := y[pId][key]; exist {
				log.Debugf("txn %v has conflict on key %v on partition %v with txn %v",
					txnId, key, pId, conflictTxnId)
				return true
			}
		}
	}

	return false
}

func (s *GTSStorageWithReorder) reorder(op *ReadAndPrepareOp) bool {
	txnId := op.request.Txn.TxnId
	canReorder := true
	conflictTxnList := s.graph.GetConflictTxn(txnId)
	log.Debugf("txn %v conflict txn: %v", txnId, conflictTxnList)
	for _, conflictTxn := range conflictTxnList {
		if conflictTxn != txnId && s.hasConflictOnOtherPartition(txnId, conflictTxn) {
			canReorder = false
		}
	}
	return canReorder
}

func (s *GTSStorageWithReorder) abortProcessedTxn(txnId string) {
	switch s.txnStore[txnId].status {
	case PREPARED:
		log.Infof("ABORT: %v (coordinator) PREPARED", txnId)
		s.txnStore[txnId].status = ABORT
		s.replicateCommitResult(txnId, nil)
		s.graph.RemoveNode(txnId, s.txnStore[txnId].readAndPrepareRequestOp.allKeys)
		s.releaseKeyAndCheckPrepare(txnId)
		break
	case WAITING:
		log.Infof("ABORT: %v (coordinator) INIT", txnId)
		s.txnStore[txnId].status = ABORT
		s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
		s.replicateCommitResult(txnId, nil)
		s.graph.RemoveNode(txnId, s.txnStore[txnId].readAndPrepareRequestOp.allKeys)
		s.releaseKeyAndCheckPrepare(txnId)
		break
	default:
		log.Fatalf("txn %v should be in statue prepared or init, but status is %v",
			txnId, s.txnStore[txnId].status)
		break
	}
}

func (s *GTSStorageWithReorder) Prepare(op *ReadAndPrepareOp) {
	log.Infof("PROCESSING txn %v", op.request.Txn.TxnId)
	txnId := op.request.Txn.TxnId
	if info, exist := s.txnStore[txnId]; exist && info.status == ABORT {
		if !info.receiveFromCoordinator {
			log.Fatalf("txn %v is aborted. it must receive coordinator abort", op.request.Txn.TxnId)
		}
		log.Infof("txn %v is already aborted (coordinator abort)", op.request.Txn.TxnId)
		s.setReadResult(op)
		return
	}

	// add to the dependency graph
	s.graph.AddNode(txnId, op.allKeys)

	s.txnStore[txnId] = &TxnInfo{
		readAndPrepareRequestOp: op,
		status:                  INIT,
		receiveFromCoordinator:  false,
		waitingTxnKey:           0,
		waitingTxnDep:           0,
		startTime:               time.Now(),
	}

	available := s.checkKeysAvailable(op)
	canReorder := s.reorder(op)
	hasWaiting := s.hasWaitingTxn(op)
	if available && (!hasWaiting || canReorder) {
		if hasWaiting && canReorder {
			log.Debugf("txn %v can be reordered keys %v", txnId, op.keyMap)
			s.txnStore[txnId].canReorder = 1
		}
		s.prepared(op)
	} else {
		if !op.passedTimestamp {
			s.txnStore[txnId].status = WAITING
			s.addToQueue(op.keyMap, op)
		} else {
			s.txnStore[txnId].status = ABORT
			s.setReadResult(op)
			s.selfAbort(op)
		}
	}
}

func (s *GTSStorageWithReorder) Commit(op *CommitRequestOp) {
	txnId := op.request.TxnId
	log.Infof("COMMITTED: %v", txnId)
	if txnInfo, exist := s.txnStore[txnId]; !exist || txnInfo.status != PREPARED {
		log.WithFields(log.Fields{
			"txnId":  txnId,
			"status": txnInfo.status,
		}).Fatal("txn should be prepared before commit")
	}

	op.wait <- true
	s.txnStore[txnId].commitTime = time.Now()

	s.txnStore[txnId].status = COMMIT
	s.txnStore[txnId].isFastPrepare = op.request.IsFastPathSuccess
	s.replicateCommitResult(txnId, op.request.WriteKeyValList)

	s.releaseKeyAndCheckPrepare(txnId)
	s.writeToDB(op.request.WriteKeyValList)
	s.graph.RemoveNode(txnId, s.txnStore[txnId].readAndPrepareRequestOp.allKeys)

	s.txnStore[txnId].receiveFromCoordinator = true
	s.txnStore[txnId].commitOrder = s.committed
	s.committed++
	s.print()
}

func (s *GTSStorageWithReorder) applyReplicatedPrepareResult(msg ReplicationMsg) {
	if s.txnStore[msg.TxnId].receiveFromCoordinator {
		log.Debugf("txn %v already receive the result from coordinator", msg.TxnId)
		// already receive final decision from coordinator
		return
	}
	log.Debugf("txn %v fast path status %v, slow path status %v", msg.TxnId, s.txnStore[msg.TxnId].status, msg.Status)
	switch s.txnStore[msg.TxnId].status {
	case PREPARED:
		s.txnStore[msg.TxnId].status = msg.Status
		if msg.Status == ABORT {
			log.Debugf("CONFLICT: txn %v fast path prepare but slow path abort, abort", msg.TxnId)
			s.releaseKeyAndCheckPrepare(msg.TxnId)
		}
		break
	case ABORT:
		s.txnStore[msg.TxnId].status = msg.Status
		if msg.Status == PREPARED {
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

func (s *GTSStorageWithReorder) applyReplicatedCommitResult(msg ReplicationMsg) {
	log.Debugf("txn %v apply replicated commit result enable fast path, status %v, current status %v",
		msg.TxnId, msg.Status, s.txnStore[msg.TxnId].status)
	s.txnStore[msg.TxnId].receiveFromCoordinator = true
	s.txnStore[msg.TxnId].isFastPrepare = msg.IsFastPathSuccess
	switch s.txnStore[msg.TxnId].status {
	case PREPARED:
		s.releaseKeyAndCheckPrepare(msg.TxnId)
	case WAITING:
		s.removeFromQueue(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		s.setReadResult(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
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
