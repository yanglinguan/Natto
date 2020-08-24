package server

import "Carousel-GTS/rpc"

type FastAbortOp struct {
	request *rpc.FastAbortRequest
	idx     int
}

func NewFastAbortOp(request *rpc.FastAbortRequest) *FastAbortOp {
	a := &FastAbortOp{
		request: request,
		idx:     0,
	}
	return a
}

func (a *FastAbortOp) SetIndex(i int) {
	a.idx = i
}

func (a FastAbortOp) GetTxnId() string {
	return a.request.AbortRequest.TxnId
}

func (a FastAbortOp) GetTimestamp() int64 {
	return a.request.Timestamp
}

func (a FastAbortOp) GetIndex() int {
	return a.idx
}
