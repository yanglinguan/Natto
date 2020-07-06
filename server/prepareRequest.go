package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type PrepareRequestOp struct {
	request *rpc.PrepareResultRequest
}

func NewPrepareRequestOp(request *rpc.PrepareResultRequest) *PrepareRequestOp {
	p := &PrepareRequestOp{
		request: request,
	}
	return p
}

func (p *PrepareRequestOp) Execute(coordinator *Coordinator) {
	txnId := p.request.TxnId
	twoPCInfo := coordinator.initTwoPCInfoIfNotExist(txnId)

	log.Debugf("txn %v receive prepared result from partition %v result %v",
		txnId, p.request.PartitionId, p.request.PrepareStatus)

	if !coordinator.server.config.GetFastPath() && twoPCInfo.status == COMMIT {
		log.Fatalf("txn %v cannot commit without the result from partition %v", txnId, p.request.PartitionId)
		return
	}

	// if the txn already abort, do noting
	if twoPCInfo.status == ABORT {
		log.Debugf("txn %v is already abort", txnId)
		return
	}

	status := TxnStatus(p.request.PrepareStatus)

	if status == ABORT {
		twoPCInfo.status = ABORT
		coordinator.checkResult(twoPCInfo)
		return
	}

	// if there is partition prepare result, there are two cases
	// 1. fast prepare success, this partition is already handled by fast path (do nothing)
	// 2. this partition requires re-prepare, update the status and prepare result
	pId := int(p.request.PartitionId)
	if info, exist := twoPCInfo.partitionPrepareResult[pId]; exist {
		if info.counter < p.request.Counter {
			log.Debugf("txn %v counter is %v greater than current counter %v status %v",
				txnId, p.request.Counter, info.counter, status)
			twoPCInfo.partitionPrepareResult[pId] = NewPartitionStatus(status, false, p.request)
		} else if info.counter > p.request.Counter {
			log.Debugf("txn %v counter is %v less than current counter is %v status %v", txnId, p.request.Counter, info.counter, status)
			return
		} else if info.isFastPrepare {
			log.Debugf("txn %v partition %v has prepared result %v, isFastPrepare %v",
				p.request.TxnId, pId, twoPCInfo.partitionPrepareResult[pId].status,
				twoPCInfo.partitionPrepareResult[pId].isFastPrepare)
			return
		}
	} else {
		twoPCInfo.partitionPrepareResult[pId] = NewPartitionStatus(status, false, p.request)
	}

	switch status {
	case PREPARED:
		coordinator.checkResult(twoPCInfo)
	case CONDITIONAL_PREPARED:
		coordinator.conditionalPrepare(p.request)
	case REVERSE_REORDER_PREPARED:
		coordinator.reverserReorderPrepare(p.request)
	case REORDER_PREPARED:
		coordinator.reorderPrepare(p.request)
	}
}
