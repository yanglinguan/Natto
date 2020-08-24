package server

import "Carousel-GTS/rpc"

type FastCommitOp struct {
	request *rpc.FastCommitRequest
	idx     int
}

func NewFastCommitOp(request *rpc.FastCommitRequest) *FastCommitOp {
	c := &FastCommitOp{
		request: request,
		idx:     0,
	}

	return c
}

func (c *FastCommitOp) SetIndex(i int) {
	c.idx = i
}

func (c FastCommitOp) GetTxnId() string {
	return c.request.CommitRequest.TxnId
}

func (c FastCommitOp) GetTimestamp() int64 {
	return c.request.Timestamp
}

func (c FastCommitOp) GetIndex() int {
	return c.idx
}
