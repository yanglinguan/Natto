package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadAndPrepareLowPriority struct {
	*ReadAndPreparePriority
}

func NewReadAndPrepareLowPriority(request *rpc.ReadAndPrepareRequest, server *Server) *ReadAndPrepareLowPriority {
	return &ReadAndPrepareLowPriority{NewReadAndPreparePriority(request, server)}
}

func (o *ReadAndPrepareLowPriority) Execute(storage *Storage) {
	logrus.Debugf("txn %v execute low priority", o.txnId)

	if storage.checkAbort(o) {
		logrus.Debugf("txn %v is ready abort", o.txnId)
		storage.setReadResult(o, -1, false)
		return
	}

	storage.AddTxn(o)
	storage.setReadResult(o, -1, false)

	// first check if keys are available and if there is waiting txn (only high priority txn will wait)
	if o.selfAbort {
		logrus.Debugf("txn %v is already self abort by scheduler", o.txnId)
		storage.selfAbort(o, EARLY_ABORT)
		return
	}

	available := storage.checkKeysAvailable(o)
	if !available {
		logrus.Debugf("txn %v key is not available, abort", o.txnId)
		storage.selfAbort(o, CONFLICT_ABORT)
		return
	}
	wait := storage.hasWaitingTxn(o)
	if wait {
		logrus.Debugf("txn %v there is waiting txn, abort", o.txnId)
		storage.selfAbort(o, WAITING_ABORT)
		return
	}

	logrus.Debugf("txn %v prepared", o.txnId)
	storage.prepare(o)
}
