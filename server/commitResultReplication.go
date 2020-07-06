package server

import (
	"Carousel-GTS/rpc"
	"bytes"
	"encoding/gob"
	log "github.com/sirupsen/logrus"
)

type CommitResultReplication struct {
	txnId     string
	writeData []*rpc.KeyValue
}

func NewCommitResultReplication(txnId string, writeData []*rpc.KeyValue) *CommitResultReplication {
	c := &CommitResultReplication{
		txnId:     txnId,
		writeData: make([]*rpc.KeyValue, len(writeData)),
	}

	for i, kv := range writeData {
		c.writeData[i] = kv
	}

	return c
}

func (c CommitResultReplication) Execute(storage *Storage) {
	if !storage.server.config.GetReplication() {
		log.Debugf("txn %v config no replication", c.txnId)
		return
	}
	log.Debugf("txn %v replicate commit result %v", c.txnId, storage.txnStore[c.txnId].status)

	replicationMsg := ReplicationMsg{
		TxnId:             c.txnId,
		Status:            storage.txnStore[c.txnId].status,
		MsgType:           CommitResultMsg,
		IsFromCoordinator: false,
		WriteData:         c.writeData,
		IsFastPathSuccess: storage.txnStore[c.txnId].isFastPrepare,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replicationMsg); err != nil {
		log.Errorf("replication encoding error: %v", err)
	}

	storage.server.raft.raftInputChannel <- string(buf.Bytes())
}
