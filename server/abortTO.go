package server

import "Carousel-GTS/rpc"

type AbortTO struct {
	*AbortOCC
}

func NewAbortTO(abortRequest *rpc.AbortRequest) *AbortTO {
	return &AbortTO{NewAbortOCC(abortRequest)}
}
