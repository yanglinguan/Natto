package server

import (
	"Carousel-GTS/rpc"
)

type ReadAndPrepareOp struct {
	request *rpc.ReadAndPrepareRequest
	wait    chan bool
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.

	// read result will send to client
	reply *rpc.ReadAndPrepareReply

	// for prepare
	readKeyMap          map[string]bool
	preparedReadKeyNum  int
	writeKeyMap         map[string]bool
	preparedWriteKeyNum int

	// prepare result will send to coordinator
	prepareResult *rpc.PrepareResultRequest

	// for commit
	numPartitions int

	sendToCoordinator bool
}

func (o *ReadAndPrepareOp) RecordPreparedKey(key string, keyType KeyType) {
	switch keyType {
	case READ:
		o.readKeyMap[key] = true
		o.preparedReadKeyNum++
	case WRITE:
		o.writeKeyMap[key] = true
		o.preparedWriteKeyNum++
	}
}

func (o *ReadAndPrepareOp) IsPrepared() bool {
	return o.preparedReadKeyNum == len(o.readKeyMap) && o.preparedWriteKeyNum == len(o.writeKeyMap)
}

func (o *ReadAndPrepareOp) BlockOwner() bool {
	return <-o.wait
}

func (o *ReadAndPrepareOp) GetReply() *rpc.ReadAndPrepareReply {
	return o.reply
}

type CommitRequestOp struct {
	request   *rpc.CommitRequest
	canCommit bool
	wait      chan bool
	result    bool
}

func (c *CommitRequestOp) BlockOwner() bool {
	return <-c.wait
}

type AbortRequestOp struct {
	abortRequest      *rpc.AbortRequest
	request           *ReadAndPrepareOp
	isFromCoordinator bool
	sendToCoordinator bool
}

func (o *AbortRequestOp) GetTxnId() string {
	if o.abortRequest != nil {
		return o.abortRequest.TxnId
	}
	if o.request != nil {
		return o.request.request.Txn.TxnId
	}

	return ""
}

type PrepareResultOp struct {
	Request          *rpc.PrepareResultRequest
	CoordPartitionId int
}
