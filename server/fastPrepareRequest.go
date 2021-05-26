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
		txnId,
		p.request.PrepareResult.PartitionId,
		TxnStatus(p.request.PrepareResult.PrepareStatus).String())
	if twoPCInfo.status.IsAbort() {
		log.Debugf("txn %v is already abort abort reason %v",
			txnId, twoPCInfo.status.String())
		return
	}

	pId := int(p.request.PrepareResult.PartitionId)

	if _, exist := twoPCInfo.fastPathPreparePartition[pId]; !exist {
		twoPCInfo.fastPathPreparePartition[pId] = NewFastPrepareStatus(txnId, pId, coordinator.server.config.GetSuperMajority())
	}

	twoPCInfo.fastPathPreparePartition[pId].addFastPrepare(p)

	ok, status := twoPCInfo.fastPathPreparePartition[pId].isFastPathSuccess()

	if pResult, exist := twoPCInfo.partitionPrepareResult[pId]; exist {
		log.Debugf("txn %v partition %v has prepared result %v, isFastPrepare %v",
			txnId, pId, twoPCInfo.partitionPrepareResult[pId].status.String(),
			twoPCInfo.partitionPrepareResult[pId].isFastPrepare)
		if ok {
			pResult.isFastPrepare = true
		}
		return
	}

	if !ok {
		log.Debugf("txn %v pId %v fast path not success yet", txnId, pId)
		return
	}
	log.Debugf("txn %v pId %v fast path success, status %v", txnId, pId, status.String())
	leaderPrepareRequest := twoPCInfo.fastPathPreparePartition[pId].leaderResult.request.PrepareResult
	twoPCInfo.partitionPrepareResult[pId] = NewPartitionStatus(status, true, leaderPrepareRequest)
	if status.IsAbort() {
		twoPCInfo.status = status
	}
	coordinator.checkResult(twoPCInfo)
}
