package server

import (
	"Carousel-GTS/rpc"
)

type ForwardReadRequestToCoordinator struct {
	request *rpc.ForwardReadToCoordinator
}

func NewForwardReadRequestToCoordinator(request *rpc.ForwardReadToCoordinator) *ForwardReadRequestToCoordinator {
	r := &ForwardReadRequestToCoordinator{
		request: request,
	}
	return r
}

func (r *ForwardReadRequestToCoordinator) Execute(c *Coordinator) {
	start := 0
	for i, txnId := range r.request.ParentTxns {
		twoPCInfo := c.initTwoPCInfoIfNotExist(txnId)
		num := int(r.request.Idx[i])
		keys := make([]string, 0)
		for j := 0; j < num; j++ {
			idx := start + j
			key := r.request.KeyList[idx]
			keys = append(keys, key)
		}
		start += num
		if twoPCInfo.writeDataReceived {
			c.sendReadResultToClient(twoPCInfo, r.request.TxnId, r.request.ClientId, keys)
		} else {
			twoPCInfo.waitingWriteDataTxn[r.request.TxnId] = &forwardReadWaiting{
				keyList:  keys,
				clientId: r.request.ClientId,
			}
		}
		twoPCInfo.notifyTxns[r.request.TxnId] = int(r.request.CoorId)
	}
}
