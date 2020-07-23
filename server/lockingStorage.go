package server

import "github.com/sirupsen/logrus"

func (s *Storage) isOldest(op *ReadAndPrepare2PL) bool {
	for key := range op.GetKeyMap() {
		nextWaitingTxn := s.kvStore.GetNextWaitingTxn(key)
		if nextWaitingTxn != nil && !op.isOlder(nextWaitingTxn) {
			logrus.Debugf("txn %v is younger than txn %v so it is not the oldest one",
				op.txnId, nextWaitingTxn.GetTxnId())
			return false
		}
	}
	return true
}

func (s *Storage) hasYoungerPrepare(op *ReadAndPrepare2PL) bool {
	for rk := range op.GetReadKeys() {
		for txnId := range s.kvStore.GetTxnHoldWrite(rk) {
			if op.isOlder(s.txnStore[txnId].readAndPrepareRequestOp) {
				logrus.Debugf("txn %v is older than txn %v which is prepared",
					op.txnId, txnId)
				return true
			}
		}
	}

	for wk := range op.GetWriteKeys() {
		for txnId := range s.kvStore.GetTxnHoldWrite(wk) {
			if op.isOlder(s.txnStore[txnId].readAndPrepareRequestOp) {
				logrus.Debugf("txn %v is older than txn %v which is prepared",
					op.txnId, txnId)
				return true
			}
		}
		for txnId := range s.kvStore.GetTxnHoldRead(wk) {
			if op.isOlder(s.txnStore[txnId].readAndPrepareRequestOp) {
				logrus.Debugf("txn %v is older than txn %v which is prepared",
					op.txnId, txnId)
				return true
			}
		}
	}

	return false
}

func (s *Storage) woundYoungerTxn(op *ReadAndPrepare2PL) {
	availableReadKeys := make(map[string]bool)
	availableWriteKeys := make(map[string]bool)
	for rk := range op.GetReadKeys() {
		if !s.kvStore.IsTxnHoldWrite(rk) {
			availableReadKeys[rk] = true
		}
	}

	for wk := range op.GetWriteKeys() {
		if !s.kvStore.IsTxnHoldWrite(wk) && !s.kvStore.IsTxnHoldRead(wk) {
			availableWriteKeys[wk] = true
		}
	}

	woundTxn := make(map[string]bool)
	for key := range availableReadKeys {
		top := s.kvStore.GetNextWaitingTxn(key)
		if top == nil {
			continue
		}
		if _, exist := woundTxn[top.GetTxnId()]; exist {
			continue
		}
		if op.isOlder(top) && top.IsReadKeyAvailable(key) {
			logrus.Debugf("txn %v wound txn %v",
				op.txnId, top.GetTxnId())
			woundTxn[top.GetTxnId()] = true
			s.setReadResult(top, -1, false)
			s.selfAbort(top, WOUND_ABORT)
		}
	}

	for key := range availableWriteKeys {
		top := s.kvStore.GetNextWaitingTxn(key)
		if top == nil {
			continue
		}
		if _, exist := woundTxn[top.GetTxnId()]; exist {
			continue
		}
		if op.isOlder(top) && top.IsWriteKeyAvailable(key) {
			logrus.Debugf("txn %v wound txn %v",
				op.txnId, top.GetTxnId())
			woundTxn[top.GetTxnId()] = true
			s.setReadResult(top, -1, false)
			s.selfAbort(top, WOUND_ABORT)
		}
	}
}
