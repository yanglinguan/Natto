package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadOnlyTO struct {
	*ReadAndPrepareTO
}

func NewReadOnlyTO(request *rpc.ReadAndPrepareRequest) *ReadOnlyTO {
	return &ReadOnlyTO{NewReadAndPrepareTO(request)}
}

func (o *ReadOnlyTO) Execute(storage *Storage) {
	logrus.Debugf("txn %v read only start execute", o.txnId)

	if !storage.server.config.GetIsReadOnly() {
		o.ReadAndPrepareTO.Execute(storage)
		return
	}

	storage.AddTxn(o)
	available := storage.checkKeysAvailable(o)
	if !available {
		storage.setReadResult(o, CONFLICT_ABORT, true)
		return
	}

	for rk := range o.readKeyList {
		if o.GetTimestamp() < storage.kvStore.GetWriteTS(rk) {
			storage.setReadResult(o, READTS_ABORT, true)
			return
		}
	}

	for wk := range o.writeKeyList {
		if o.GetTimestamp() < storage.kvStore.GetReadTS(wk) ||
			o.GetTimestamp() < storage.kvStore.GetWriteTS(wk) {
			storage.setReadResult(o, WRITETS_ABORT, true)
			return
		}
	}

	storage.setReadResult(o, PREPARED, true)
}
