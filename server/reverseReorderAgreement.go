package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReverseReorderAgreement struct {
	request *rpc.ReverseAgreementRequest
}

func NewReverseReorderAgreement(request *rpc.ReverseAgreementRequest) *ReverseReorderAgreement {
	return &ReverseReorderAgreement{request: request}
}

func (r ReverseReorderAgreement) Execute(coordinator *Coordinator) {
	logrus.Debugf("txn %v receive reverse agreement from txn %v canReverse %v partitionId %v %v",
		r.request.TxnId, r.request.ReorderedTxnId, r.request.AgreeReorder, r.request.PartitionId)

	twoPCInfo := coordinator.initTwoPCInfoIfNotExist(r.request.TxnId)
	pId := int(r.request.PartitionId)

	if _, exist := twoPCInfo.partitionPrepareResult[pId]; !exist {
		twoPCInfo.partitionPrepareResult[pId] = NewPartitionStatus(REVERSE_REORDER_PREPARED, false, nil)
		twoPCInfo.partitionPrepareResult[pId].counter = r.request.Counter
	}

	if twoPCInfo.partitionPrepareResult[pId].prepareResult != nil {
		if twoPCInfo.partitionPrepareResult[pId].counter > r.request.Counter {
			logrus.Debugf("txn %v counter %v > agreement counter %v (txn %v reorderedTxn %v)",
				r.request.TxnId, twoPCInfo.partitionPrepareResult[pId].prepareResult.Counter,
				r.request.Counter, r.request.TxnId, r.request.ReorderedTxnId)
			return
		}
	}

	if !r.request.AgreeReorder {
		coordinator.sendRePrepare(r.request.TxnId, r.request.ReorderedTxnId, pId, r.request.Counter)
		return
	}

	twoPCInfo.partitionPrepareResult[pId].reorderAgreementReceived[r.request.ReorderedTxnId] = true

	if twoPCInfo.partitionPrepareResult[pId].prepareResult == nil {
		logrus.Debugf("txn %v does not receive prepare result from partition %v",
			r.request.TxnId, pId)
		return
	}

	for _, txn := range twoPCInfo.partitionPrepareResult[pId].prepareResult.Reorder {
		if _, exist := twoPCInfo.partitionPrepareResult[pId].reorderAgreementReceived[txn]; !exist {
			logrus.Debugf("txn %v cannot prepare because txn %v does not reverse reorder on partition %v",
				r.request.TxnId, txn, pId)
			return
		}
	}

	logrus.Debugf("txn %v can prepare on partition %v all reordered txn are reversed", r.request.TxnId, pId)
	twoPCInfo.partitionPrepareResult[pId].status = PREPARED

	coordinator.checkResult(twoPCInfo)
}
