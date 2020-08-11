package server

import "github.com/sirupsen/logrus"

type Release struct {
	txnId string
}

func NewRelease(txnId string) *Release {
	return &Release{txnId: txnId}
}

func (r *Release) Execute(storage *Storage) {
	op, ok := storage.txnStore[r.txnId].readAndPrepareRequestOp.(LockingOp)
	if !ok {
		logrus.Fatalf("txn %v should be read only gts")
	}
	storage.Release(op)
}
