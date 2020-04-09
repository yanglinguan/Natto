package server

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type GTSStorageDepGraph struct {
	*AbstractStorage

	graph *Graph

	// txnId set
	readyToCommitTxn map[string]bool
	// txnId -> commitRequestOp
	waitToCommitTxn map[string]*CommitRequestOp
}

func NewGTSStorageDepGraph(server *Server) *GTSStorageDepGraph {
	s := &GTSStorageDepGraph{
		AbstractStorage:  NewAbstractStorage(server),
		graph:            NewDependencyGraph(),
		readyToCommitTxn: make(map[string]bool),
		waitToCommitTxn:  make(map[string]*CommitRequestOp),
	}

	s.AbstractStorage.abstractMethod = s

	return s
}

func (s *GTSStorageDepGraph) getNextCommitListByCommitOrAbort(txnId string) {
	log.Debugf("REMOVE %v", txnId)
	s.graph.RemoveNode(txnId, s.txnStore[txnId].readAndPrepareRequestOp.keyMap)

	for _, txn := range s.graph.GetNext() {
		log.Debugf("txn %v can commit now", txn)
		if _, exist := s.readyToCommitTxn[txn]; exist {
			log.Debugf("txnId is already in ready to commit txn")
			continue
		}

		if _, exist := s.waitToCommitTxn[txn]; exist {
			s.waitToCommitTxn[txn].canCommit = true
			//s.Commit(s.waitToCommitTxn[txn])
			s.server.executor.CommitTxn <- s.waitToCommitTxn[txn]
			delete(s.waitToCommitTxn, txn)
		} else {
			s.readyToCommitTxn[txn] = true
		}
	}
	delete(s.readyToCommitTxn, txnId)
}

func (s *GTSStorageDepGraph) checkCommit(txnId string) bool {
	if len(s.readyToCommitTxn) == 0 {
		for _, txn := range s.graph.GetNext() {
			log.Debugf("txn %v can commit now", txn)
			s.readyToCommitTxn[txn] = true
		}
	}

	if _, exist := s.readyToCommitTxn[txnId]; exist {
		return true
	} else {
		return false
	}
}

func (s *GTSStorageDepGraph) Commit(op *CommitRequestOp) {
	txnId := op.request.TxnId

	if txnInfo, exist := s.txnStore[txnId]; !exist || txnInfo.status != PREPARED {
		log.WithFields(log.Fields{
			"txnId":  txnId,
			"status": txnInfo.status,
		}).Fatal("txn should be prepared before commit")
	}

	if op.canCommit || s.checkCommit(txnId) {
		log.Infof("COMMIT %v", txnId)
		op.wait <- true

		s.txnStore[txnId].status = COMMIT
		s.txnStore[txnId].isFastPrepare = op.request.IsFastPathSuccess
		s.replicateCommitResult(txnId, op.request.WriteKeyValList)

		s.getNextCommitListByCommitOrAbort(txnId)
		s.releaseKeyAndCheckPrepare(txnId)
		s.writeToDB(op.request.WriteKeyValList)

		s.txnStore[txnId].receiveFromCoordinator = true
		s.txnStore[txnId].commitOrder = s.committed
		s.committed++
		s.print()
	} else {
		log.Debugf("WAIT COMMIT %v", txnId)
		s.waitToCommitTxn[txnId] = op
	}
}

func (s *GTSStorageDepGraph) abortProcessedTxn(txnId string) {
	switch s.txnStore[txnId].status {
	case PREPARED:
		log.Infof("ABORT %v (coordinator) PREPARED", txnId)
		s.txnStore[txnId].status = ABORT
		s.replicateCommitResult(txnId, nil)
		s.getNextCommitListByCommitOrAbort(txnId)
		s.releaseKeyAndCheckPrepare(txnId)
		break
	case WAITING:
		log.Infof("ABORT %v (coordinator) INIT", txnId)
		s.txnStore[txnId].status = ABORT
		s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
		s.replicateCommitResult(txnId, nil)
		s.getNextCommitListByCommitOrAbort(txnId)
		s.releaseKeyAndCheckPrepare(txnId)
		break
	default:
		log.Fatalf("txn %v should be in statue prepared or init, but status is %v",
			txnId, s.txnStore[txnId].status)
		break
	}
}

// return true if txnId1 < txnId2
func (s *GTSStorageDepGraph) less(txnId1 string, txnId2 string) bool {
	request1 := s.txnStore[txnId1].readAndPrepareRequestOp.request
	request2 := s.txnStore[txnId2].readAndPrepareRequestOp.request
	if request1.Timestamp == request2.Timestamp {
		if request1.ClientId == request2.ClientId {
			return txnId1 < txnId2
		}
		return request1.ClientId < request1.ClientId
	}
	return request1.Timestamp < request2.Timestamp
}

func (s *GTSStorageDepGraph) checkKeysAvailable(op *ReadAndPrepareOp) bool {
	// write read conflict
	for rk := range op.readKeyMap {
		for txnId := range s.kvStore[rk].PreparedTxnWrite {
			if s.less(txnId, op.txnId) {
				log.Debugf("txn %v read write conflict key %v with older %v", op.txnId, rk, s.kvStore[rk].PreparedTxnWrite)
				return false
			}
		}
	}
	return true
}

// return true if there is a waiting txn has write read conflict with the txn
func (s *GTSStorageDepGraph) checkWaitingTxnHasWriteReadConflict(op *ReadAndPrepareOp) bool {
	for rk := range op.readKeyMap {
		for txnId := range s.kvStore[rk].WaitingItem {
			if _, exist := s.txnStore[txnId].readAndPrepareRequestOp.writeKeyMap[rk]; exist {
				return true
			}
		}
	}
	return false
}

func (s *GTSStorageDepGraph) prepared(op *ReadAndPrepareOp) {
	log.Infof("DEP graph prepared %v", op.txnId)
	s.removeFromQueue(op)
	txnId := op.txnId
	s.txnStore[txnId].status = PREPARED
	s.setReadResult(op)
	if op.request.Txn.ReadOnly && s.server.config.GetIsReadOnly() {
		if s.txnStore[txnId].inQueue {
			s.server.executor.ReleaseReadOnlyTxn <- op
		}
		return
	}
	s.recordPrepared(op)
	s.setPrepareResult(op)
	s.replicatePreparedResult(txnId)
}

func (s *GTSStorageDepGraph) Prepare(op *ReadAndPrepareOp) {
	log.Infof("PROCESSING %v", op.txnId)
	txnId := op.txnId
	if info, exist := s.txnStore[txnId]; exist && info.status != INIT {
		log.Infof("txn %v is already has status %v", txnId, info.status)
		s.setReadResult(op)
		return
	}

	s.txnStore[txnId] = &TxnInfo{
		readAndPrepareRequestOp: op,
		status:                  INIT,
		receiveFromCoordinator:  false,
		commitOrder:             0,
	}
	s.txnStore[txnId].startTime = time.Now()

	available := s.checkKeysAvailable(op)
	writeReadConflict := s.checkWaitingTxnHasWriteReadConflict(op)
	hasWaiting := s.hasWaitingTxn(op)

	canPrepare := available && !writeReadConflict
	if s.server.IsLeader() && !op.request.Txn.ReadOnly && (canPrepare || !op.passedTimestamp) {
		if s.graph.AddNode(txnId, op.keyMap) {
			s.readyToCommitTxn[txnId] = true
		}
	}

	if canPrepare {
		if hasWaiting && !writeReadConflict {
			s.txnStore[txnId].hasWaitingButNoWriteReadConflict = true
		}
		s.prepared(op)
	} else {
		if !op.passedTimestamp {
			s.txnStore[txnId].status = WAITING
			log.Debugf("txn %v cannot prepare available %v", txnId, available)
			s.addToQueue(op.keyMap, op)
		} else {
			s.txnStore[txnId].status = ABORT
			log.Debugf("txn %v passed timestamp also cannot prepared", txnId)
			//	s.setReadResult(op)
			s.selfAbort(op)
		}
	}
}

func (s *GTSStorageDepGraph) applyReplicatedPrepareResult(msg ReplicationMsg) {
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
			log.Debugf("txn %v fast path prepare but slow path abort, abort", msg.TxnId)
			s.releaseKeyAndCheckPrepare(msg.TxnId)
		}
		break
	case ABORT:
		s.txnStore[msg.TxnId].status = msg.Status
		if msg.Status == PREPARED {
			log.Debugf("txn %v fast path abort but slow path prepare, prepare", msg.TxnId)
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
	case INIT:
		log.Debugf("txn %v fast path not stated slow path status %v ", msg.TxnId, msg.Status)
		s.txnStore[msg.TxnId].status = msg.Status
		if msg.Status == PREPARED {
			s.recordPrepared(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		}
		break
	}
}

func (s *GTSStorageDepGraph) applyReplicatedCommitResult(msg ReplicationMsg) {
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
