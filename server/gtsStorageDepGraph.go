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
	delete(s.readyToCommitTxn, txnId)
	for _, txn := range s.graph.GetNext() {
		log.Debugf("txn %v can commit now", txn)
		s.readyToCommitTxn[txn] = true
		if _, exist := s.waitToCommitTxn[txn]; exist {
			s.waitToCommitTxn[txn].canCommit = true
			//s.Commit(s.waitToCommitTxn[txn])
			s.server.executor.CommitTxn <- s.waitToCommitTxn[txn]
			delete(s.waitToCommitTxn, txn)
		}
	}
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

func (s *GTSStorageDepGraph) checkKeysAvailable(op *ReadAndPrepareOp) bool {
	available := true
	// read write conflict
	for rk := range op.readKeyMap {
		if len(s.kvStore[rk].PreparedTxnWrite) > 0 {
			log.Debugf("txn %v read write conflict key %v with %v", op.request.Txn.TxnId, rk, s.kvStore[rk].PreparedTxnWrite)
			available = false
			break
		}
	}
	return available
}

func (s *GTSStorageDepGraph) prepared(op *ReadAndPrepareOp) {
	log.Infof("DEP graph prepared %v", op.request.Txn.TxnId)
	s.removeFromQueue(op)
	txnId := op.request.Txn.TxnId
	s.graph.AddNode(txnId, op.keyMap)
	s.txnStore[txnId].status = PREPARED
	s.recordPrepared(op)
	s.setReadResult(op)
	s.setPrepareResult(op)
	s.replicatePreparedResult(txnId)
}

func (s *GTSStorageDepGraph) Prepare(op *ReadAndPrepareOp) {
	log.Infof("PROCESSING %v", op.request.Txn.TxnId)
	txnId := op.request.Txn.TxnId
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

	available := s.checkKeysAvailable(op)
	hasWaiting := s.hasWaitingTxn(op)

	if available && !hasWaiting {
		s.prepared(op)
	} else {
		if !op.passedTimestamp {
			s.txnStore[txnId].status = WAITING
			log.Debugf("txn %v cannot prepare available %v, hasWaiting %v", txnId, available, hasWaiting)
			s.addToQueue(op.keyMap, op)
		} else {
			s.txnStore[txnId].status = ABORT
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
			s.graph.AddNode(msg.TxnId, s.txnStore[msg.TxnId].readAndPrepareRequestOp.keyMap)
			s.recordPrepared(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		}
	case INIT:
		log.Debugf("txn %v fast path not stated slow path status %v ", msg.TxnId, msg.Status)
		s.txnStore[msg.TxnId].status = msg.Status
		if msg.Status == PREPARED {
			s.graph.AddNode(msg.TxnId, s.txnStore[msg.TxnId].readAndPrepareRequestOp.keyMap)
			s.recordPrepared(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
		}
		break
	}
}

func (s *GTSStorageDepGraph) applyReplicatedCommitResult(msg ReplicationMsg) {
	log.Debugf("txn %v apply replicated commit result enable fast path, status %v, current status %v",
		msg.TxnId, msg.Status, s.txnStore[msg.TxnId].status)
	s.txnStore[msg.TxnId].receiveFromCoordinator = true
	s.getNextCommitListByCommitOrAbort(msg.TxnId)
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
