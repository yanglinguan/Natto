package server

import (
	"bytes"
	"encoding/gob"
	log "github.com/sirupsen/logrus"
)

type WriteDataReplication struct {
	txnId string
}

func NewWriteDataReplication(txnId string) *WriteDataReplication {
	return &WriteDataReplication{txnId: txnId}
}

func (w WriteDataReplication) Execute(coordinator *Coordinator) {
	if !coordinator.server.config.GetReplication() {
		coordinator.txnStore[w.txnId].writeDataReplicated = true
		log.Debugf("txn %v config not replication", w.txnId)
		return
	}

	replicationMsg := ReplicationMsg{
		TxnId:             w.txnId,
		Status:            coordinator.txnStore[w.txnId].status,
		MsgType:           WriteDataMsg,
		WriteData:         coordinator.txnStore[w.txnId].commitRequest.request.WriteKeyValList,
		IsFromCoordinator: true,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replicationMsg); err != nil {
		log.Errorf("replication encoding error: %v", err)
	}
	log.Debugf("txn %v replicated write data size %v", w.txnId, len(buf.Bytes()))
	coordinator.server.raft.raftInputChannel <- string(buf.Bytes())
}
