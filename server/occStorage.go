package server

import (
	log "github.com/sirupsen/logrus"
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

func (s *OccStorage) Prepare(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	if txnInfo, exist := s.txnStore[txnId]; exist && txnInfo.status == ABORT {
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
		s.txnStore[txnId].status = PREPARED
		s.recordPrepared(op)
		s.setPrepareResult(op)
	} else {
		s.txnStore[txnId].status = ABORT
		abortOp := NewAbortRequestOp(nil, op, false)
		s.server.executor.AbortTxn <- abortOp
	}
}

func (s *OccStorage) Commit(op *CommitRequestOp) {
	txnId := op.request.TxnId
	log.Infof("COMMIT %v", txnId)
	s.release(txnId)
	s.writeToDB(op)
	s.txnStore[txnId].status = COMMIT
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
		s.release(txnId)
		s.txnStore[txnId].status = ABORT
		break
	default:
		log.Fatalf("txn %v should be in statue prepared, but status is %v",
			txnId, s.txnStore[txnId].status)
		break
	}

}

func (s *OccStorage) Abort(op *AbortRequestOp) {
	if op.isFromCoordinator {
		s.coordinatorAbort(op.abortRequest)
	} else {
		op.sendToCoordinator = !s.txnStore[op.request.request.Txn.TxnId].receiveFromCoordinator
		if op.sendToCoordinator {
			s.setPrepareResult(op.request)
		}
	}
}
