package server

import (
	"Carousel-GTS/rpc"
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

	return s
}

func (s *GTSStorageDepGraph) getNextCommitListByCommitOrAbort(txnId string) {
	log.Debugf("REMOVE %v", txnId)
	s.graph.Remove(txnId)
	delete(s.readyToCommitTxn, txnId)
	for _, txn := range s.graph.GetNext() {
		log.Debugf("txn %v can commit now", txn)
		s.readyToCommitTxn[txn] = true
		if _, exist := s.waitToCommitTxn[txn]; exist {
			s.waitToCommitTxn[txn].canCommit = true
			s.Commit(s.waitToCommitTxn[txn])
			//s.server.executor.CommitTxn <- s.waitToCommitTxn[txn]
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

		s.release(txnId)
		s.writeToDB(op)

		s.txnStore[txnId].status = COMMIT
		s.txnStore[txnId].receiveFromCoordinator = true
		s.txnStore[txnId].commitOrder = s.committed
		s.committed++
		s.getNextCommitListByCommitOrAbort(txnId)
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

func (s *GTSStorageDepGraph) coordinatorAbort(request *rpc.AbortRequest) {
	txnId := request.TxnId
	if txnInfo, exist := s.txnStore[txnId]; exist {
		txnInfo.receiveFromCoordinator = true
		switch txnInfo.status {
		case ABORT:
			log.Infof("txn %v is already abort it self", txnId)
			break
		case COMMIT:
			log.Fatalf("Error: txn %v is already committed", txnId)
			break
		default:
			log.Debugf("call abort processed txn %v", txnId)
			s.abortProcessedTxn(txnId)
			break
		}
	} else {
		log.Infof("ABORT %v (coordinator init txnInfo)", txnId)

		s.txnStore[txnId] = &TxnInfo{
			readAndPrepareRequestOp: nil,
			status:                  ABORT,
			receiveFromCoordinator:  true,
		}
	}
}

func (s *GTSStorageDepGraph) Abort(op *AbortRequestOp) {
	if op.isFromCoordinator {
		s.coordinatorAbort(op.abortRequest)
	} else {
		s.selfAbort(op.request)
		s.setReadResult(op.request)
		op.sendToCoordinator = !s.txnStore[op.request.request.Txn.TxnId].receiveFromCoordinator
		if op.sendToCoordinator {
			s.setPrepareResult(op.request, ABORT)
		}
	}
}

func (s *GTSStorageDepGraph) checkKeysAvailable(op *ReadAndPrepareOp) bool {
	available := true
	// read write conflict
	for rk := range op.readKeyMap {
		if len(s.kvStore[rk].PreparedTxnWrite) > 0 {
			available = false
			break
		}
	}
	return available
}

func (s *GTSStorageDepGraph) prepared(op *ReadAndPrepareOp) {
	log.Infof("DEP graph prepared %v", op.request.Txn.TxnId)
	s.addToGraph(op, true)
	s.recordPrepared(op)
	s.setReadResult(op)
	s.setPrepareResult(op, PREPARED)
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
		s.addToQueue(op.keyMap, op)
	}
}

func (s *GTSStorageDepGraph) addToGraph(op *ReadAndPrepareOp, addWrite bool) {
	txnId := op.request.Txn.TxnId
	// only add to dependent graph when txn can be prepared
	if !op.IsPrepared() {
		log.WithFields(log.Fields{
			"txnId":      txnId,
			"isPrepared": false,
		}).Fatalln("cannot add a txn that is not prepared into dependency graph")
	}

	s.graph.AddNode(txnId)

	for wk := range op.writeKeyMap {

		for txn := range s.kvStore[wk].PreparedTxnRead {
			if txn != txnId {
				s.graph.AddEdge(txn, txnId)
			}
		}

		if addWrite {
			for txn := range s.kvStore[wk].PreparedTxnWrite {
				if txn != txnId {
					s.graph.AddEdge(txn, txnId)
				}
			}
		}
	}
}
