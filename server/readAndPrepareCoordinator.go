package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type ReadAndPrepareCoordinator struct {
	request *rpc.ReadAndPrepareRequest
}

func NewReadAndPrepareCoordinator(request *rpc.ReadAndPrepareRequest) *ReadAndPrepareCoordinator {
	return &ReadAndPrepareCoordinator{request: request}
}

func (o ReadAndPrepareCoordinator) Execute(coordinator *Coordinator) {
	txnId := o.request.Txn.TxnId
	log.Debugf("receive read and prepare from client %v", txnId)
	twoPCInfo := coordinator.initTwoPCInfoIfNotExist(txnId)

	twoPCInfo.readAndPrepareOp = o.request

	coordinator.checkResult(twoPCInfo)
}
