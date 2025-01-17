package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type CommitCoordinator struct {
	request    *rpc.CommitRequest
	clientWait chan bool
	result     bool
}

func NewCommitCoordinator(request *rpc.CommitRequest) *CommitCoordinator {
	return &CommitCoordinator{
		request:    request,
		clientWait: make(chan bool, 1),
	}
}

func (c *CommitCoordinator) blockClient() {
	<-c.clientWait
}

func (c *CommitCoordinator) unblockClient() {
	c.clientWait <- true
}

func (c *CommitCoordinator) Execute(coordinator *Coordinator) {
	txnId := c.request.TxnId
	log.Debugf("receive commit from client %v", txnId)

	twoPCInfo := coordinator.initTwoPCInfoIfNotExist(txnId)
	twoPCInfo.writeDataReceived = true
	twoPCInfo.commitRequestOp = c
	for i, kv := range c.request.WriteKeyValList {
		twoPCInfo.writeDataMap[kv.Key] = kv
		twoPCInfo.writeDataFromLeader[kv.Key] = c.request.ResultFromLeader[i]
	}

	for tId, clientInfo := range twoPCInfo.waitingWriteDataTxn {
		coordinator.sendReadResultToClient(twoPCInfo, tId, clientInfo.clientId, clientInfo.keyList)
	}

	if twoPCInfo.status.IsAbort() || twoPCInfo.status == COMMIT {
		log.Debugf("TXN %v already aborted status %v", txnId, twoPCInfo.status.String())
		c.result = twoPCInfo.status == COMMIT
		c.unblockClient()
		return
	}

	if len(c.request.WriteKeyValList) == 0 && coordinator.server.config.GetIsReadOnly() {
		log.Debugf("txn %v is readOnly, do not need to replicate write data", txnId)
		twoPCInfo.writeDataReplicated = true
	} else {
		writeDataReplication := NewWriteDataReplication(txnId)
		coordinator.AddOperation(writeDataReplication)
	}

	coordinator.checkResult(twoPCInfo)
}
