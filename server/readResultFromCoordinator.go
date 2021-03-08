package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

// op created when client send the stream rpc to coordinator
// to receive the read result
// this op will be only created once
type ReadRequestFromCoordinator struct {
	request *rpc.ReadRequestToCoordinator
	stream  rpc.Carousel_ReadResultFromCoordinatorServer
}

func NewReadRequestFromCoordinator(request *rpc.ReadRequestToCoordinator, stream rpc.Carousel_ReadResultFromCoordinatorServer) *ReadRequestFromCoordinator {
	r := &ReadRequestFromCoordinator{
		request: request,
		stream:  stream,
	}
	return r
}

func (r *ReadRequestFromCoordinator) Execute(c *Coordinator) {
	c.clientReadRequestToCoordinator[r.request.ClientId] = r.stream
	err := c.clientReadRequestToCoordinator[r.request.ClientId].Send(&rpc.ReadReplyFromCoordinator{
		KeyValVerList: nil,
		TxnId:         "ACK",
	})
	if err != nil {
		logrus.Fatalf("cannot send ack to client %v", r.request.ClientId)
	}
}
