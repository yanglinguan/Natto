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
		txnId, p.request.PartitionId, TxnStatus(p.request.PrepareStatus).String())

	//if !coordinator.server.config.GetFastPath() && twoPCInfo.status == COMMIT {
	//	log.Fatalf("txn %v cannot commit without the result from partition %v", txnId, p.request.PartitionId)
	//	return
	//}

	// if the txn already abort, do noting
	if twoPCInfo.status.IsAbort() {
		log.Debugf("txn %v is already abort abort reason %v",
			txnId, twoPCInfo.status.String())
		return
	}

	if twoPCInfo.status == COMMIT {
		log.Debugf("txn %v is already commit", txnId)
		if twoPCInfo.status == PREPARED &&
			!coordinator.server.config.GetFastPath() && !twoPCInfo.conditionPrepare {
			log.Fatalf("txn %v is not fast and not conditional prepare", txnId)
		}
		return
	}

	status := TxnStatus(p.request.PrepareStatus)

	if status.IsAbort() {
		twoPCInfo.status = status
		coordinator.checkResult(twoPCInfo)
		return
	}

	// if there is partition prepare result, there are two cases
	// 1. fast prepare success, this partition is already handled by fast path (do nothing)
	// 2. this partition requires re-prepare, update the status and prepare result
	pId := int(p.request.PartitionId)
	if info, exist := twoPCInfo.partitionPrepareResult[pId]; exist {
		if info.counter < p.request.Counter {
			if twoPCInfo.partitionPrepareResult[pId].status == CONDITIONAL_PREPARED {
				r := twoPCInfo.partitionPrepareResult[pId].prepareResult
				for c := range r.Conditions {
					twoPCInfo.conditionGraph.RemoveEdge(c, int(r.PartitionId))
				}
			}
			log.Debugf("txn %v counter is %v greater than current counter %v status %v",
				txnId, p.request.Counter, info.counter, status.String())
			twoPCInfo.partitionPrepareResult[pId] = NewPartitionStatus(status, false, p.request)
		} else if info.counter > p.request.Counter {
			log.Debugf("txn %v counter is %v less than current counter is %v status %v",
				txnId, p.request.Counter, info.counter, status.String())
			if twoPCInfo.partitionPrepareResult[pId].prepareResult == nil {
				twoPCInfo.partitionPrepareResult[pId].prepareResult = p.request
				log.Debugf("txn %v does not receive prepare result from partition %v assign the request",
					p.request.TxnId, pId)
			} else {
				return
			}
		} else if info.fastPrepareUsed {
			log.Debugf("txn %v partition %v has prepared result %v, isFastPrepare %v",
				p.request.TxnId, pId, twoPCInfo.partitionPrepareResult[pId].status.String(),
				twoPCInfo.partitionPrepareResult[pId].fastPrepareUsed)
			return
		}
	} else {
		twoPCInfo.partitionPrepareResult[pId] = NewPartitionStatus(status, false, p.request)
	}

	switch status {
	case PREPARED, EARLY_ABORT_PREPARED:
		if status == EARLY_ABORT_PREPARED {
			twoPCInfo.hasEarlyAbort = true
		}

		if txns, exist := twoPCInfo.dependTxnByPId[pId]; exist {
			for depTxn := range txns {
				delete(twoPCInfo.dependTxns, depTxn)
			}
			delete(twoPCInfo.dependTxnByPId, pId)
		}
		coordinator.checkResult(twoPCInfo)
	case CONDITIONAL_PREPARED:
		twoPCInfo.conditionPrepare = true
		coordinator.conditionalPrepare(p.request)
	case REVERSE_REORDER_PREPARED:
		twoPCInfo.reversedReorderPrepare = true
		coordinator.reverserReorderPrepare(p.request)
	case REORDER_PREPARED:
		twoPCInfo.reorderPrepare = true
		coordinator.reorderPrepare(p.request)
	case FORWARD_PREPARED:
		twoPCInfo.forwardPrepare = true
		coordinator.forwardPrepare(p.request)
	}
}
