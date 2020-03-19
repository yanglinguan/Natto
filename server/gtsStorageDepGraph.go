package server

import (
	log "github.com/sirupsen/logrus"
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

		s.getNextCommitListByCommitOrAbort(txnId)
		s.release(txnId)
		s.writeToDB(op)

		s.txnStore[txnId].status = COMMIT
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
		s.getNextCommitListByCommitOrAbort(txnId)
		s.release(txnId)
		break
	case INIT:
		log.Infof("ABORT %v (coordinator) INIT", txnId)
		s.txnStore[txnId].status = ABORT
		s.getNextCommitListByCommitOrAbort(txnId)
		s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
		s.release(txnId)
		break
	default:
		log.Fatalf("txn %v should be in statue prepared or init, but status is %v",
			txnId, s.txnStore[txnId].status)
		break
	}
}

//func (s *GTSStorageDepGraph) Abort(op *AbortRequestOp) {
//	if op.isFromCoordinator {
//		s.coordinatorAbort(op.abortRequest)
//	} else {
//		//s.selfAbort(op.request)
//		//s.setReadResult(op.request)
//		op.sendToCoordinator = !s.txnStore[op.request.request.Txn.TxnId].receiveFromCoordinator
//		if op.sendToCoordinator {
//			s.setPrepareResult(op.request)
//		}
//	}
//}

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
	s.graph.AddNode(op.request.Txn.TxnId, op.keyMap)
	s.txnStore[op.request.Txn.TxnId].status = PREPARED
	s.recordPrepared(op)
	s.setReadResult(op)
	s.setPrepareResult(op)
}

func (s *GTSStorageDepGraph) Prepare(op *ReadAndPrepareOp) {
	log.Infof("PROCESSING %v", op.request.Txn.TxnId)
	txnId := op.request.Txn.TxnId
	if info, exist := s.txnStore[txnId]; exist && info.status == ABORT {
		if !info.receiveFromCoordinator {
			log.Fatalf("txn %v is aborted. it must receive coordinator abort", op.request.Txn.TxnId)
		}
		log.Infof("txn %v is already aborted (coordinator abort)", op.request.Txn.TxnId)
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
			log.Debugf("txn %v cannot prepare available %v, hasWaiting %v", txnId, available, hasWaiting)
			s.addToQueue(op.keyMap, op)
		} else {
			s.txnStore[txnId].status = ABORT
			s.setReadResult(op)
			s.selfAbort(op)
		}
	}
}
