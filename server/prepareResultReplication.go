package server

import (
	"bytes"
	"encoding/gob"
	log "github.com/sirupsen/logrus"
)

type PrepareResultReplicationOp struct {
	txnId string
}

func NewPrepareResultReplicationOp(txnId string) *PrepareResultReplicationOp {
	p := &PrepareResultReplicationOp{
		txnId: txnId,
	}
	return p
}

func (p *PrepareResultReplicationOp) Execute(storage *Storage) {
	if !storage.server.config.GetReplication() {
		log.Debugf("txn %v config no replication send result to coordinator", p.txnId)
		//s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
		storage.sendPrepareResult(p.txnId, storage.txnStore[p.txnId].status)
		//s.readyToSendPrepareResultToCoordinator(s.txnStore[txnId].prepareResultOp)
		return
	}

	//Replicates the prepare result to followers.
	replicationMsg := ReplicationMsg{
		TxnId:             p.txnId,
		Status:            storage.txnStore[p.txnId].status,
		MsgType:           PrepareResultMsg,
		IsFromCoordinator: false,
		HighPriority:      storage.txnStore[p.txnId].readAndPrepareRequestOp.GetPriority(),
	}

	if !replicationMsg.Status.IsAbort() {
		if replicationMsg.Status == CONDITIONAL_PREPARED {
			replicationMsg.PreparedReadKeyVersion = storage.txnStore[p.txnId].conditionalPrepareResultRequest.ReadKeyVerList
			replicationMsg.PreparedWriteKeyVersion = storage.txnStore[p.txnId].conditionalPrepareResultRequest.WriteKeyVerList
			replicationMsg.Conditions = storage.txnStore[p.txnId].conditionalPrepareResultRequest.Conditions
		} else {
			replicationMsg.PreparedReadKeyVersion = storage.txnStore[p.txnId].prepareResultRequest.ReadKeyVerList
			replicationMsg.PreparedWriteKeyVersion = storage.txnStore[p.txnId].prepareResultRequest.WriteKeyVerList
		}
	}

	//if replicationMsg.Status == CONDITIONAL_PREPARED {
	//	replicationMsg.Conditions = storage.txnStore[p.txnId].prepareResultRequest.Conditions
	//}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replicationMsg); err != nil {
		log.Errorf("replication encoding error: %v", err)
	}

	log.Debugf("txn %s replicates the prepare result %v.", p.txnId, storage.txnStore[p.txnId].status)

	storage.server.raft.raftInputChannel <- string(buf.Bytes())
}
