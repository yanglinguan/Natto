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

	readKeyList  map[string]bool // true if readKey is available
	writeKeyList map[string]bool // true if writeKey is available

	keyMap map[string]bool // true: key is available

	highPriority bool

	index         int
	passTimestamp bool
}

func NewReadAndPrepareBase(request *rpc.ReadAndPrepareRequest) *ReadAndPrepareBase {
	o := &ReadAndPrepareBase{
		txnId:        "",
		request:      request,
		reply:        nil,
		clientWait:   make(chan bool, 0),
		readKeyList:  make(map[string]bool),
		writeKeyList: make(map[string]bool),
		highPriority: false,
		keyMap:       make(map[string]bool),
	}

	if request == nil {
		return o
	}
	o.txnId = request.Txn.TxnId
	o.highPriority = request.Txn.HighPriority

	for _, key := range request.Txn.ReadKeyList {
		o.keyMap[key] = false
		o.readKeyList[key] = false
	}

	for _, key := range request.Txn.WriteKeyList {
		o.keyMap[key] = false
		o.writeKeyList[key] = false
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
		readKeyList:  make(map[string]bool),
		writeKeyList: make(map[string]bool),
		keyMap:       make(map[string]bool),
	}

	for _, kv := range msg.PreparedReadKeyVersion {
		o.readKeyList[kv.Key] = false
		o.keyMap[kv.Key] = false
	}

	for _, kv := range msg.PreparedWriteKeyVersion {
		o.writeKeyList[kv.Key] = false
		o.keyMap[kv.Key] = false
	}

	return o
}

func (o *ReadAndPrepareBase) GetReadKeys() map[string]bool {
	return o.readKeyList
}

func (o *ReadAndPrepareBase) GetWriteKeys() map[string]bool {
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

func (o *ReadAndPrepareBase) GetKeyMap() map[string]bool {
	return o.keyMap
}

func (o *ReadAndPrepareBase) SetReadKeyAvailable(key string) {
	o.readKeyList[key] = true
}

func (o *ReadAndPrepareBase) SetWriteKeyAvailable(key string) {
	o.writeKeyList[key] = true
}

func (o *ReadAndPrepareBase) IsReadKeyAvailable(key string) bool {
	return o.readKeyList[key]
}

func (o *ReadAndPrepareBase) IsWriteKeyAvailable(key string) bool {
	return o.writeKeyList[key]
}

func (o *ReadAndPrepareBase) GetIndex() int {
	return o.index
}

func (o *ReadAndPrepareBase) SetIndex(i int) {
	o.index = i
}

func (o *ReadAndPrepareBase) SetPassTimestamp() {
	o.passTimestamp = true
}

func (o *ReadAndPrepareBase) IsPassTimestamp() bool {
	return o.passTimestamp
}
