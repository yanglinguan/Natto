package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type ReadAndPrepareOCC struct {
	txnId string

	// client prepareResult
	request *rpc.ReadAndPrepareRequest
	// read result will send to client
	reply *rpc.ReadAndPrepareReply
	// client will block on this chan until read prepareResult is ready
	clientWait chan bool

	readKeyList  []string
	writeKeyList []string
	highPriority bool
}

func NewReadAndPrepareOCC(request *rpc.ReadAndPrepareRequest) *ReadAndPrepareOCC {
	o := &ReadAndPrepareOCC{
		txnId:        request.Txn.TxnId,
		request:      request,
		reply:        nil,
		clientWait:   make(chan bool, 1),
		readKeyList:  make([]string, len(request.Txn.ReadKeyList)),
		writeKeyList: make([]string, len(request.Txn.WriteKeyList)),
	}

	for i, k := range request.Txn.ReadKeyList {
		o.readKeyList[i] = k
	}

	for i, k := range request.Txn.ReadKeyList {
		o.writeKeyList[i] = k
	}

	return o
}

func NewReadAndPrepareOCCWithReplicationMsg(msg ReplicationMsg) *ReadAndPrepareOCC {
	o := &ReadAndPrepareOCC{
		txnId:        msg.TxnId,
		request:      nil,
		reply:        nil,
		clientWait:   nil,
		highPriority: msg.HighPriority,
		readKeyList:  make([]string, len(msg.PreparedReadKeyVersion)),
		writeKeyList: make([]string, len(msg.PreparedWriteKeyVersion)),
	}

	for i, kv := range msg.PreparedReadKeyVersion {
		o.readKeyList[i] = kv.Key
	}

	for i, kv := range msg.PreparedWriteKeyVersion {
		o.writeKeyList[i] = kv.Key
	}

	return o
}

func (o *ReadAndPrepareOCC) Execute(storage *Storage) {
	log.Debugf("txn %v start execute", o.txnId)

	if storage.checkAbort(o) {
		log.Debugf("txn %v is ready abort", o.txnId)
		return
	}

	storage.AddTxn(o)
	storage.setReadResult(o, -1, false)

	available := storage.checkKeysAvailable(o)
	if available {
		storage.prepare(o)
	} else {
		storage.selfAbort(o)
	}
}

func (o *ReadAndPrepareOCC) Schedule(schedule *Scheduler) {
	schedule.server.storage.AddOperation(o)
}

func (o *ReadAndPrepareOCC) GetPriority() bool {
	return o.highPriority
}

func (o *ReadAndPrepareOCC) GetTxnId() string {
	return o.txnId
}

func (o *ReadAndPrepareOCC) GetReadKeys() []string {
	return o.readKeyList
}

func (o *ReadAndPrepareOCC) GetWriteKeys() []string {
	return o.writeKeyList
}

func (o *ReadAndPrepareOCC) GetKeyMap() map[string]bool {
	return nil
}

func (o *ReadAndPrepareOCC) SetReadReply(reply *rpc.ReadAndPrepareReply) {
	o.reply = reply
}

func (o *ReadAndPrepareOCC) UnblockClient() {
	o.clientWait <- true
}

func (o *ReadAndPrepareOCC) GetReadReply() *rpc.ReadAndPrepareReply {
	return o.reply
}

func (o *ReadAndPrepareOCC) BlockClient() {
	<-o.clientWait
}

func (o *ReadAndPrepareOCC) GetCoordinatorPartitionId() int {
	return int(o.request.Txn.CoordPartitionId)
}

func (o *ReadAndPrepareOCC) GetReadRequest() *rpc.ReadAndPrepareRequest {
	return o.request
}

func (o *ReadAndPrepareOCC) GetTimestamp() int64 {
	return o.request.Timestamp
}
