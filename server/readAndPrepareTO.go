package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadAndPrepareTO struct {
	*ReadAndPrepareBase
}

func NewReadAndPrepareTO(request *rpc.ReadAndPrepareRequest) *ReadAndPrepareTO {
	o := &ReadAndPrepareTO{NewReadAndPrepareBase(request)}
	return o
}

func NewReadAndPrepareTOWithReplicationMsg(msg *ReplicationMsg) *ReadAndPrepareTO {
	o := &ReadAndPrepareTO{
		NewReadAndPrepareBaseWithReplicationMsg(msg),
	}

	return o
}

func (o *ReadAndPrepareTO) Execute(storage *Storage) {
	logrus.Debugf("txn %v start execute", o.txnId)

	if storage.checkAbort(o) {
		logrus.Debugf("txn %v is ready abort", o.txnId)
		return
	}

	storage.AddTxn(o)
	storage.setReadResult(o, -1, false)
	available := storage.checkKeysAvailable(o)
	if !available {
		storage.selfAbort(o, CONFLICT_ABORT)
		return
	}

	// check timestamp
	for rk := range o.readKeyList {
		if o.GetTimestamp() < storage.kvStore.GetWriteTS(rk) {
			storage.selfAbort(o, READTS_ABORT)
			return
		}
	}

	for wk := range o.writeKeyList {
		if o.GetTimestamp() < storage.kvStore.GetReadTS(wk) ||
			o.GetTimestamp() < storage.kvStore.GetWriteTS(wk) {
			storage.selfAbort(o, WRITETS_ABORT)
			return
		}
	}

	storage.prepare(o)
}
