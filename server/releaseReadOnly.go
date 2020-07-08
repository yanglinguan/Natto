package server

import "github.com/sirupsen/logrus"

type ReleaseReadOnly struct {
	txnId string
}

func NewReleaseReadOnly(txnId string) *ReleaseReadOnly {
	return &ReleaseReadOnly{txnId: txnId}
}

func (r *ReleaseReadOnly) Execute(storage *Storage) {
	op, ok := storage.txnStore[r.txnId].readAndPrepareRequestOp.(*ReadOnlyGTS)
	if !ok {
		logrus.Fatalf("txn %v should be read only gts")
	}
	storage.ReleaseReadOnly(op)
}
