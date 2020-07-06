package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type FastPrepareRequestOp struct {
	request *rpc.FastPrepareResultRequest
}

func NewFastPrepareRequestOp(request *rpc.FastPrepareResultRequest) *FastPrepareRequestOp {
	p := &FastPrepareRequestOp{
		request: request,
	}
	return p
}

func (p *FastPrepareRequestOp) Execute(coordinator *Coordinator) {
	txnId := p.request.PrepareResult.TxnId
	twoPCInfo := coordinator.initTwoPCInfoIfNotExist(txnId)
	log.Debugf("txn %v receive fast prepared result from partition %v result %v",
		txnId, p.request.PrepareResult.TxnId, p.request.PrepareResult.PrepareStatus)
	if twoPCInfo.status == ABORT {
		log.Debugf("txn %v is already abort", txnId)
		return
	}

	pId := int(p.request.PrepareResult.PartitionId)
	if _, exist := twoPCInfo.partitionPrepareResult[pId]; exist {
		log.Debugf("txn %v partition %v has prepared result %v, isFastPrepare %v",
			txnId, pId, twoPCInfo.partitionPrepareResult[pId].status, twoPCInfo.partitionPrepareResult[pId].isFastPrepare)
		return
	}

	if _, exist := twoPCInfo.fastPathPreparePartition[pId]; !exist {
		twoPCInfo.fastPathPreparePartition[pId] = NewFastPrepareStatus(txnId, pId, coordinator.server.config.GetSuperMajority())
	}

	twoPCInfo.fastPathPreparePartition[pId].addFastPrepare(p)

	ok, status := twoPCInfo.fastPathPreparePartition[pId].isFastPathSuccess()

	if !ok {
		log.Debugf("txn %v pId %v fast path not success yet", txnId, pId)
		return
	}
	log.Debugf("txn %v pId %v fast path success", txnId, pId)
	leaderPrepareRequest := twoPCInfo.fastPathPreparePartition[pId].leaderResult.request.PrepareResult
	switch status {
	case ABORT:
		twoPCInfo.status = ABORT
		coordinator.checkResult(twoPCInfo)
	case PREPARED:
		coordinator.checkResult(twoPCInfo)
	case CONDITIONAL_PREPARED:
		coordinator.conditionalPrepare(leaderPrepareRequest)
	case REVERSE_REORDER_PREPARED:
		coordinator.reverserReorderPrepare(leaderPrepareRequest)
	case REORDER_PREPARED:
		coordinator.reorderPrepare(leaderPrepareRequest)
	}
	//if status == ABORT {
	//	twoPCInfo.status = ABORT
	//} else {
	//	twoPCInfo.partitionPrepareResult[pId] = NewPartitionStatus(status, true, p.request.PrepareResult)
	//}
	//
	//coordinator.checkResult(twoPCInfo)
}
