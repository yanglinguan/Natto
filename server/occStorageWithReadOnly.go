package server

type OccStorageWithReadOnly struct {
	*OccStorage
}

func NewOccStorageWithReadOnly(server *Server) *OccStorageWithReadOnly {
	s := &OccStorageWithReadOnly{NewOccStorage(server)}
	return s
}

func (s *OccStorageWithReadOnly) Prepare(op *ReadAndPrepareOp) {
	txnId := op.txnId
	if txnInfo, exist := s.txnStore[txnId]; exist && txnInfo.status != INIT {
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
		if len(op.writeKeyMap) == 0 {
			s.setReadResult(op)
		}
	} else {
		s.txnStore[txnId].status = ABORT
		s.selfAbort(op)
	}
}
