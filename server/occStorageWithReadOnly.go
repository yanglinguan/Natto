package server

type OccStorageWithReadOnly struct {
	*OccStorage
}

func NewOccStorageWithReadOnly(server *Server) *OccStorageWithReadOnly {
	s := &OccStorageWithReadOnly{NewOccStorage(server)}
	return s
}

func (s *OccStorageWithReadOnly) prepared(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	s.txnStore[txnId].status = PREPARED
	s.recordPrepared(op)
	s.setReadResult(op)
	s.txnStore[txnId].prepareResultOp = s.setPrepareResult(op)
	s.replicatePreparedResult(op.request.Txn.TxnId)
}

func (s *OccStorageWithReadOnly) Prepare(op *ReadAndPrepareOp) {
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

	if len(op.writeKeyMap) > 0 {
		s.setReadResult(op)
	}

	available := s.checkKeysAvailable(op)
	if available {
		s.prepared(op)
	} else {
		s.txnStore[txnId].status = ABORT
		s.selfAbort(op)
	}
	// read only txn
	//if len(op.writeKeyMap) == 0 {
	//	s.setReadResult(op)
	//}
}
