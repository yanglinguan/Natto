package server

import (
	"Carousel-GTS/rpc"
	"Carousel-GTS/utils"
	"log"
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
		if twoPCInfo.writeDataReceived {
			stream := c.clientReadRequestToCoordinator[r.request.ClientId]
			readResult := &rpc.ReadReplyFromCoordinator{
				KeyValVerList: make([]*rpc.KeyValueVersion, 0),
				TxnId:         r.request.TxnId,
			}
			num := int(r.request.Idx[i])
			for j := 0; j < num; j++ {
				idx := start + j
				key := r.request.KeyList[idx]
				value := twoPCInfo.writeDataMap[key].Value
				r := &rpc.KeyValueVersion{
					Key:     key,
					Value:   value,
					Version: uint64(utils.ConvertToInt(value)),
				}
				readResult.KeyValVerList = append(readResult.KeyValVerList, r)
			}
			start += num
			err := stream.Send(readResult)
			if err != nil {
				log.Fatalf("stream send read result error %v txn %v ", err, r.request.TxnId)
			}
		}
		twoPCInfo.notifyTxns[r.request.TxnId] = int(r.request.CoorId)
	}
}
