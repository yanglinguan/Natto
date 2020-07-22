package server

import "github.com/sirupsen/logrus"

func (s *Storage) isOldest(op *ReadAndPrepare2PL) bool {
	for key := range op.GetKeyMap() {
		nextWaitingTxn := s.kvStore.GetNextWaitingTxn(key)
		if !op.isOlder(nextWaitingTxn) {
			logrus.Debugf("txn %v is younger than txn %v so it is not the oldest one",
				op.txnId, nextWaitingTxn.GetTxnId())
			return false
		}
	}
	return true
}

func (s *Storage) hasYoungerPrepare(op *ReadAndPrepare2PL) bool {
	for _, rk := range op.GetReadKeys() {
		for txnId := range s.kvStore.GetTxnHoldWrite(rk) {
			if op.isOlder(s.txnStore[txnId].readAndPrepareRequestOp) {
				logrus.Debugf("txn %v is older than txn %v which is prepared",
					op.txnId, txnId)
				return true
			}
		}
	}

	for _, wk := range op.GetWriteKeys() {
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
