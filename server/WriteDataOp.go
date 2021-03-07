package server

import (
	"Carousel-GTS/rpc"
	"bytes"
	"encoding/gob"
	"github.com/sirupsen/logrus"
)

type WriteDataOp struct {
	writeData *rpc.WriteDataRequest
}

func NewWriteDataOp(request *rpc.WriteDataRequest) *WriteDataOp {
	op := &WriteDataOp{writeData: request}
	return op
}

func (op *WriteDataOp) Execute(coordinator *Coordinator) {
	txnId := op.writeData.TxnId
	twoPCInfo := coordinator.initTwoPCInfoIfNotExist(txnId)
	for _, kv := range op.writeData.WriteKeyValList {
		twoPCInfo.writeDataMap[kv.Key] = kv
		twoPCInfo.writeDataFromLeader[kv.Key] = true
	}

	if !coordinator.server.config.GetReplication() {
		coordinator.txnStore[op.writeData.TxnId].writeDataReplicated = true
		logrus.Debugf("txn %v config not replication", txnId)
		return
	}

	replicationMsg := ReplicationMsg{
		TxnId:             op.writeData.TxnId,
		Status:            coordinator.txnStore[txnId].status,
		MsgType:           WriteDataMsg,
		WriteData:         op.writeData.WriteKeyValList,
		IsFromCoordinator: true,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replicationMsg); err != nil {
		logrus.Errorf("replication encoding error: %v", err)
	}
	logrus.Debugf("txn %v replicated write data size %v", op.writeData.TxnId, len(buf.Bytes()))
	coordinator.server.raft.raftInputChannel <- string(buf.Bytes())
}
