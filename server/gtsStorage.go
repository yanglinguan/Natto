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
	if txnInfo, exist := s.txnStore[txnId]; !exist || txnInfo.status != PREPARED {
		log.WithFields(log.Fields{
			"txnId":  txnId,
			"status": txnInfo.status,
		}).Fatal("txn should be prepared before commit")
	}

	op.wait <- true

	s.txnStore[txnId].status = COMMIT

	s.replicateCommitResult(txnId, op.request.WriteKeyValList)

	s.txnStore[txnId].commitTime = time.Now()

	s.release(txnId)

	s.writeToDB(op.request.WriteKeyValList)

	s.txnStore[txnId].receiveFromCoordinator = true
	s.txnStore[txnId].commitOrder = s.committed
	s.committed++
	s.print()
}

func (s *GTSStorage) abortProcessedTxn(txnId string) {
	switch s.txnStore[txnId].status {
	case PREPARED:
		log.Infof("ABORT: %v (coordinator) PREPARED", txnId)
		s.txnStore[txnId].status = ABORT
		s.replicateCommitResult(txnId, nil)
		s.release(txnId)
		break
	case INIT:
		log.Infof("ABORT: %v (coordinator) INIT", txnId)
		s.txnStore[txnId].status = ABORT
		s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
		s.replicateCommitResult(txnId, nil)
		s.release(txnId)
		break
	default:
		log.Fatalf("txn %v should be in statue prepared or init, but status is %v",
			txnId, s.txnStore[txnId].status)
		break
	}
}

func (s *GTSStorage) Prepare(op *ReadAndPrepareOp) {
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

	canPrepare := s.checkKeysAvailable(op)

	hasWaiting := s.hasWaitingTxn(op)

	if canPrepare && !hasWaiting {
		s.prepared(op)
	} else {
		if !op.passedTimestamp {
			s.addToQueue(op.keyMap, op)
		} else {
			s.txnStore[txnId].status = ABORT
			//s.setReadResult(op)
			s.selfAbort(op)
		}
	}
}
