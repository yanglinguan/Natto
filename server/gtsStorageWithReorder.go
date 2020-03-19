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
		s.graph.RemoveNode(txnId, s.txnStore[txnId].readAndPrepareRequestOp.allKeys)
		s.release(txnId)
		break
	case INIT:
		log.Infof("ABORT: %v (coordinator) INIT", txnId)
		s.txnStore[txnId].status = ABORT
		s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
		s.graph.RemoveNode(txnId, s.txnStore[txnId].readAndPrepareRequestOp.allKeys)
		s.release(txnId)
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

	s.release(txnId)
	s.writeToDB(op)
	s.graph.RemoveNode(txnId, s.txnStore[txnId].readAndPrepareRequestOp.allKeys)

	s.txnStore[txnId].status = COMMIT
	s.txnStore[txnId].receiveFromCoordinator = true
	s.txnStore[txnId].commitOrder = s.committed
	s.committed++
	s.print()
}

//func (s *GTSStorageWithReorder) Abort(op *AbortRequestOp) {
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
