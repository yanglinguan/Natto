package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadAndPrepare2PL struct {
	*ReadAndPrepareBase

	//keyMap map[string]bool
	//index int // The index of the item in the heap.
}

func NewReadAndPrepareLock2PL(request *rpc.ReadAndPrepareRequest) *ReadAndPrepare2PL {
	op := &ReadAndPrepare2PL{
		ReadAndPrepareBase: NewReadAndPrepareBase(request),
		//keyMap:             make(map[string]bool),
	}

	for _, key := range request.Txn.ReadKeyList {
		op.keyMap[key] = true
	}

	for _, key := range request.Txn.WriteKeyList {
		op.keyMap[key] = true
	}

	return op
}

func NewReadAndPrepare2PLWithReplicationMsg(msg *ReplicationMsg) *ReadAndPrepare2PL {
	op := &ReadAndPrepare2PL{
		ReadAndPrepareBase: NewReadAndPrepareBaseWithReplicationMsg(msg),
	}

	return op
}

func (o *ReadAndPrepare2PL) Execute(storage *Storage) {
	logrus.Debugf("txn %v execute", o.txnId)

	if storage.checkAbort(o) {
		logrus.Debugf("txn %v already abort", o.txnId)
		storage.setReadResult(o, -1, false)
		return
	}

	storage.AddTxn(o)

	available := storage.checkKeysAvailable(o)
	waiting := storage.hasWaitingTxn(o)
	if storage.server.config.UseNetworkTimestamp() {
		// when timestamp is used deadlock is prevented
		if available && !waiting {
			storage.setReadResult(o, -1, false)
			storage.prepare(o)
			return
		}
		if o.IsPassTimestamp() {
			storage.setReadResult(o, -1, false)
			storage.selfAbort(o, PASS_TIMESTAMP_ABORT)
			return
		}

		storage.wait(o)

		return
	}

	if storage.hasYoungerPrepare(o) {
		storage.setReadResult(o, -1, false)
		storage.selfAbort(o, WOUND_ABORT)
		return
	}

	if available && !waiting {
		storage.setReadResult(o, -1, false)
		storage.prepare(o)
		return
	}
	//else if available {
	//	if storage.isOldest(o) {
	//		storage.setReadResult(o, -1, false)
	//		storage.prepare(o)
	//		return
	//	}
	//} else {
	//	if storage.hasYoungerPrepare(o) {
	//		storage.setReadResult(o, -1, false)
	//		storage.selfAbort(o, WOUND_ABORT)
	//		return
	//	}
	//}

	storage.wait(o)

}

func (o *ReadAndPrepare2PL) executeFromQueue(storage *Storage) bool {
	if storage.server.config.UseNetworkTimestamp() {
		return o.executeFromQueueWithNetworkTimestamp(storage)
	}

	if storage.hasYoungerPrepare(o) {
		storage.setReadResult(o, -1, false)
		storage.selfAbort(o, WOUND_ABORT)
		storage.removeFromQueue(o)
		releaseOp := NewRelease(o.txnId)
		storage.AddOperation(releaseOp)
		return true
	}

	waiting := storage.hasWaitingTxn(o)
	available := storage.checkKeysAvailable(o)
	if available && !waiting {
		storage.setReadResult(o, -1, false)
		storage.removeFromQueue(o)
		storage.prepare(o)
		return true
	}

	return false
}

func (o *ReadAndPrepare2PL) executeFromQueueWithNetworkTimestamp(storage *Storage) bool {
	waiting := storage.hasWaitingTxn(o)
	if waiting {
		logrus.Debugf("txn %v cannot prepare because should has older txn before it", o.txnId)
		return false
	}

	available := storage.checkKeysAvailable(o)
	if !available {
		logrus.Debugf("txn %v cannot prepare because key are not available", o.txnId)
		return false
	}
	storage.setReadResult(o, -1, false)
	storage.removeFromQueue(o)
	storage.prepare(o)
	return true
}

func (o *ReadAndPrepare2PL) isOlder(other ReadAndPrepareOp) bool {
	logrus.Debugf("txn %v timestamp %v other txn %v timestamp %v",
		o.txnId, o.GetTimestamp(), other.GetTxnId(), other.GetTimestamp())
	if o.GetTimestamp() == other.GetTimestamp() {
		return o.txnId < other.GetTxnId()
	}
	return o.GetTimestamp() < other.GetTimestamp()
}
