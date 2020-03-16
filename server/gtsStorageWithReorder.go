package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
	"time"
)

type GTSStorageWithReorder struct {
	*AbstractStorage
}

func NewGTSStorageWithReorder(server *Server) *GTSStorageWithReorder {
	s := &GTSStorageWithReorder{NewAbstractStorage(server)}
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

func (s GTSStorageWithReorder) hasConflictInQueue(txnId string, key string) bool {
	log.Debugf("txnId %v check has conflict in queue key %v", txnId, key)
	for e := s.kvStore[key].WaitingOp.Back(); e != nil; e = e.Prev() {
		isConflict := s.hasConflictOnOtherPartition(txnId, e.Value.(*ReadAndPrepareOp).request.Txn.TxnId)
		if isConflict {
			return true
		}
	}
	return false
}

func (s *GTSStorageWithReorder) reorder(op *ReadAndPrepareOp) bool {
	txnId := op.request.Txn.TxnId
	canReorder := true
	for key := range op.keyMap {
		if s.hasConflictInQueue(txnId, key) {
			canReorder = false
			break
		}
	}
	return canReorder
}

func (s *GTSStorageWithReorder) abortProcessedTxn(txnId string) {
	switch s.txnStore[txnId].status {
	case PREPARED:
		log.Infof("ABORT: %v (coordinator) PREPARED", txnId)
		s.txnStore[txnId].status = ABORT
		s.release(txnId)
		break
	case INIT:
		log.Infof("ABORT: %v (coordinator) INIT", txnId)
		s.txnStore[txnId].status = ABORT
		s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
		s.release(txnId)
		break
	default:
		log.Fatalf("txn %v should be in statue prepared or init, but status is %v",
			txnId, s.txnStore[txnId].status)
		break
	}
}

func (s *GTSStorageWithReorder) coordinatorAbort(request *rpc.AbortRequest) {
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
	if available && canReorder {
		if hasWaiting {
			log.Debugf("txn %v can reorder keys", txnId, op.keyMap)
			s.txnStore[txnId].canReorder = 1
		}
		s.prepared(op)
	} else {
		s.addToQueue(op.keyMap, op)
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

	s.txnStore[txnId].status = COMMIT
	s.txnStore[txnId].receiveFromCoordinator = true
	s.txnStore[txnId].commitOrder = s.committed
	s.committed++
	s.print()
}

func (s *GTSStorageWithReorder) Abort(op *AbortRequestOp) {
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
