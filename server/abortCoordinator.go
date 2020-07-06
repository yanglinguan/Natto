package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type AbortCoordinator struct {
	abortRequest *rpc.AbortRequest
}

func NewAbortCoordinator(abortRequest *rpc.AbortRequest) *AbortCoordinator {
	a := &AbortCoordinator{
		abortRequest: abortRequest,
	}
	return a
}

func (a AbortCoordinator) Execute(coordinator *Coordinator) {
	txnId := a.abortRequest.TxnId
	log.Debugf("receive abort from client txn %v", a.abortRequest.TxnId)
	twoPCInfo := coordinator.initTwoPCInfoIfNotExist(txnId)

	twoPCInfo.abortRequest = a.abortRequest
	twoPCInfo.status = ABORT

	coordinator.checkResult(twoPCInfo)
}
