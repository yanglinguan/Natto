package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type ReadAndPrepareOCC struct {
	*ReadAndPrepareBase
}

func NewReadAndPrepareOCC(request *rpc.ReadAndPrepareRequest) *ReadAndPrepareOCC {
	o := &ReadAndPrepareOCC{
		ReadAndPrepareBase: NewReadAndPrepareBase(request),
	}

	return o
}

func NewReadAndPrepareOCCWithReplicationMsg(msg *ReplicationMsg) *ReadAndPrepareOCC {
	o := &ReadAndPrepareOCC{
		NewReadAndPrepareBaseWithReplicationMsg(msg),
	}

	return o
}

func (o *ReadAndPrepareOCC) Execute(storage *Storage) {
	log.Debugf("txn %v start execute", o.txnId)

	if storage.checkAbort(o) {
		log.Debugf("txn %v is ready abort", o.txnId)
		return
	}

	storage.AddTxn(o)
	storage.setReadResult(o, -1, false)

	available := storage.checkKeysAvailable(o)
	if available {
		storage.prepare(o)
	} else {
		storage.selfAbort(o, CONFLICT_ABORT)
	}
}
