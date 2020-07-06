package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReverseReorderRequest struct {
	request *rpc.ReverseReorderRequest
}

func NewReverseReorderRequest(request *rpc.ReverseReorderRequest) *ReverseReorderRequest {
	r := &ReverseReorderRequest{request: request}
	return r
}

func (r ReverseReorderRequest) Execute(coordinator *Coordinator) {
	logrus.Debugf("txn %v request reverse reorder of txn %v", r.request.TxnId, r.request.ReorderedTxnId)
	reorderTxnInfo := coordinator.initTwoPCInfoIfNotExist(r.request.ReorderedTxnId)

	canReverse := reorderTxnInfo.status != COMMIT

	if canReverse {
		for pId, result := range reorderTxnInfo.partitionPrepareResult {
			if result.status != REORDER_PREPARED {
				continue
			}

			coordinator.sendRePrepare(r.request.ReorderedTxnId, r.request.TxnId, pId, result.counter)
		}
	}

	coordinator.sendReverseReorderAgreement(r.request, canReverse)
}
