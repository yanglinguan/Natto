package server

import "Carousel-GTS/rpc"

type ReadAndPrepareBase struct {
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

func NewReadAndPrepareBase(request *rpc.ReadAndPrepareRequest) *ReadAndPrepareBase {
	o := &ReadAndPrepareBase{
		txnId:        request.Txn.TxnId,
		request:      request,
		reply:        nil,
		clientWait:   make(chan bool, 0),
		readKeyList:  request.Txn.ReadKeyList,
		writeKeyList: request.Txn.WriteKeyList,
		highPriority: request.Txn.HighPriority,
	}

	return o
}

func NewReadAndPrepareBaseWithReplicationMsg(msg *ReplicationMsg) *ReadAndPrepareBase {
	o := &ReadAndPrepareBase{
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

func (o *ReadAndPrepareBase) GetReadKeys() []string {
	return o.readKeyList
}

func (o *ReadAndPrepareBase) GetWriteKeys() []string {
	return o.writeKeyList
}

func (o *ReadAndPrepareBase) GetTxnId() string {
	return o.txnId
}

func (o *ReadAndPrepareBase) GetPriority() bool {
	return o.highPriority
}

func (o *ReadAndPrepareBase) GetReadReply() *rpc.ReadAndPrepareReply {
	return o.reply
}

func (o *ReadAndPrepareBase) SetReadReply(reply *rpc.ReadAndPrepareReply) {
	o.reply = reply
}

func (o *ReadAndPrepareBase) UnblockClient() {
	o.clientWait <- true
}

func (o *ReadAndPrepareBase) BlockClient() {
	<-o.clientWait
}

func (o *ReadAndPrepareBase) GetCoordinatorPartitionId() int {
	return int(o.request.Txn.CoordPartitionId)
}

func (o *ReadAndPrepareBase) GetReadRequest() *rpc.ReadAndPrepareRequest {
	return o.request
}

func (o *ReadAndPrepareBase) GetClientId() string {
	return o.request.ClientId
}

func (o *ReadAndPrepareBase) GetTimestamp() int64 {
	return o.request.Timestamp
}
