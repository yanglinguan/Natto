package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadAndPrepare2PL struct {
	*ReadAndPrepareBase

	keyMap map[string]bool
	index  int // The index of the item in the heap.
}

func NewReadAndPrepareLock2PL(request *rpc.ReadAndPrepareRequest) *ReadAndPrepare2PL {
	op := &ReadAndPrepare2PL{
		ReadAndPrepareBase: NewReadAndPrepareBase(request),
		keyMap:             make(map[string]bool),
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
		keyMap:             make(map[string]bool),
	}

	for _, kv := range msg.PreparedReadKeyVersion {
		op.keyMap[kv.Key] = true
	}

	for _, kv := range msg.PreparedWriteKeyVersion {
		op.keyMap[kv.Key] = true
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

	if available && !waiting {
		storage.setReadResult(o, -1, false)
		storage.prepare(o)
	} else if available {
		if storage.isOldest(o) {
			storage.setReadResult(o, -1, false)
			storage.prepare(o)
		} else {
			storage.wait(o)
		}
	} else {
		if storage.hasYoungerPrepare(o) {
			storage.setReadResult(o, -1, false)
			storage.selfAbort(o, WOUND_ABORT)
		} else {
			storage.wait(o)
		}
	}

}

func (o *ReadAndPrepare2PL) executeFromQueue(storage *Storage) bool {
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

func (o *ReadAndPrepare2PL) Start(server *Server) {
	server.storage.AddOperation(o)
}

func (o *ReadAndPrepare2PL) GetKeyMap() map[string]bool {
	return o.keyMap
}

// if this operation is order than other return -1 (has smaller timestamp)
// if this operation is younger than other return 1 (has larger timestamp)
// if this operation is other return 0 (other and this are the same operation)
func (o *ReadAndPrepare2PL) isOlder(other ReadAndPrepareOp) bool {
	logrus.Debugf("txn %v timestamp %v other txn $v timestamp %v",
		o.txnId, o.GetTimestamp(), other.GetTxnId(), other.GetTimestamp())
	if o.GetTimestamp() < other.GetTimestamp() {
		return true
	} else if o.txnId < other.GetTxnId() {
		return true
	}

	return o.GetClientId() < other.GetClientId()
}

func (o *ReadAndPrepare2PL) setIndex(i int) {
	o.index = i
}

func (o *ReadAndPrepare2PL) getIndex() int {
	return o.index
}
