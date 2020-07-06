package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadOnlyOCC struct {
	*ReadAndPrepareOCC
}

func NewReadOnlyOCC(request *rpc.ReadAndPrepareRequest) *ReadOnlyOCC {
	return &ReadOnlyOCC{
		ReadAndPrepareOCC: NewReadAndPrepareOCC(request),
	}
}

func (r *ReadOnlyOCC) Execute(storage *Storage) {
	logrus.Debugf("txn %v execute read only", r.request.Txn.TxnId)
	if !storage.server.config.GetIsReadOnly() {
		r.ReadAndPrepareOCC.Execute(storage)
		return
	}

	available := storage.checkKeysAvailable(r)
	status := PREPARED
	if !available {
		status = ABORT
	}
	storage.setReadResult(r, status, true)
}
