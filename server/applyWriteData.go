package server

import log "github.com/sirupsen/logrus"

type ApplyWriteData struct {
	msg *ReplicationMsg
}

func NewApplyWriteData(msg *ReplicationMsg) *ApplyWriteData {
	return &ApplyWriteData{msg: msg}
}

func (w ApplyWriteData) Execute(coordinator *Coordinator) {
	log.Debugf("txn %v apply write data replicated msg", w.msg.TxnId)
	if coordinator.server.IsLeader() {
		coordinator.txnStore[w.msg.TxnId].writeDataReplicated = true
		if coordinator.txnStore[w.msg.TxnId].status == COMMIT {
			//coordinator.sendRequest <- c.txnStore[msg.TxnId]
			coordinator.sendToParticipantsAndClient(coordinator.txnStore[w.msg.TxnId])
			//coordinator.checkResult(coordinator.txnStore[w.msg.TxnId])
		} else {
			if coordinator.server.config.ForwardReadToCoord() {
				coordinator.checkResult(coordinator.txnStore[w.msg.TxnId])
			}
		}
	} else {
		twoPCInfo := coordinator.initTwoPCInfoIfNotExist(w.msg.TxnId)
		for i, kv := range w.msg.WriteData {
			if _, exist := twoPCInfo.writeDataMap[kv.Key]; exist {
				twoPCInfo.writeDataMap[kv.Key] = kv
				twoPCInfo.writeDataFromLeader[kv.Key] = true
				continue
			}
			twoPCInfo.writeDataMap[kv.Key] = kv
			twoPCInfo.writeDataFromLeader[kv.Key] = w.msg.WriteDataFromLeader[i]
		}

		for _, kv := range w.msg.WriteData {
			coordinator.txnStore[w.msg.TxnId].writeDataMap[kv.Key] = kv
		}
		//coordinator.txnStore[w.msg.TxnId].writeData = w.msg.WriteData
	}
}
