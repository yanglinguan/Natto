package server

import "Carousel-GTS/rpc"

type ReadOnlyOCC struct {
	*ReadAndPrepareOCC
}

func NewReadOnlyOCC(request *rpc.ReadAndPrepareRequest) *ReadOnlyOCC {
	return &ReadOnlyOCC{
		ReadAndPrepareOCC: NewReadAndPrepareOCC(request),
	}
}

func (r *ReadOnlyOCC) Execute(storage *Storage) {
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
