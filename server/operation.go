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

	keyMap map[string]bool

	allKeys map[string]bool

	partitionKeys map[int]map[string]bool

	// prepare result will send to coordinator
	prepareResult *rpc.PrepareResultRequest

	// for commit
	numPartitions int

	sendToCoordinator bool

	passedTimestamp bool
}

func NewReadAndPrepareOp(request *rpc.ReadAndPrepareRequest, server *Server) *ReadAndPrepareOp {
	r := &ReadAndPrepareOp{
		request:             request,
		wait:                make(chan bool, 1),
		index:               0,
		reply:               nil,
		readKeyMap:          make(map[string]bool),
		preparedReadKeyNum:  0,
		writeKeyMap:         make(map[string]bool),
		preparedWriteKeyNum: 0,
		keyMap:              make(map[string]bool),
		prepareResult:       nil,
		numPartitions:       0,
		sendToCoordinator:   false,
		partitionKeys:       make(map[int]map[string]bool),
		allKeys:             make(map[string]bool),
		passedTimestamp:     false,
	}

	r.processKey(request.Txn.ReadKeyList, server, READ)
	r.processKey(request.Txn.WriteKeyList, server, WRITE)

	r.numPartitions = len(request.Txn.ParticipatedPartitionIds)

	return r
}

func (o *ReadAndPrepareOp) processKey(keys []string, server *Server, keyType KeyType) {
	for _, key := range keys {
		pId := server.config.GetPartitionIdByKey(key)
		if _, exist := o.partitionKeys[pId]; !exist {
			o.partitionKeys[pId] = make(map[string]bool)
		}
		o.partitionKeys[pId][key] = true
		o.allKeys[key] = true
		if !server.storage.HasKey(key) {
			continue
		}
		o.keyMap[key] = true

		if keyType == WRITE {
			o.writeKeyMap[key] = false
		} else if keyType == READ {
			o.readKeyMap[key] = false
		}
	}
}

func (o *ReadAndPrepareOp) RecordPreparedKey(key string, keyType KeyType) {
	switch keyType {
	case READ:
		o.readKeyMap[key] = true
		o.preparedReadKeyNum++
		break
	case WRITE:
		o.writeKeyMap[key] = true
		o.preparedWriteKeyNum++
		break
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

func NewCommitRequestOp(request *rpc.CommitRequest) *CommitRequestOp {
	c := &CommitRequestOp{
		request:   request,
		canCommit: false,
		wait:      make(chan bool, 1),
		result:    false,
	}

	return c
}

func (c *CommitRequestOp) BlockOwner() bool {
	return <-c.wait
}

type AbortRequestOp struct {
	abortRequest      *rpc.AbortRequest
	request           *ReadAndPrepareOp
	isFromCoordinator bool
	//sendToCoordinator bool
}

func NewAbortRequestOp(abortRequest *rpc.AbortRequest,
	request *ReadAndPrepareOp, fromCoordinator bool) *AbortRequestOp {
	a := &AbortRequestOp{
		abortRequest:      abortRequest,
		request:           request,
		isFromCoordinator: fromCoordinator,
		//sendToCoordinator: false,
	}
	return a
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

func NewPrepareRequestOp(request *rpc.PrepareResultRequest, coordinatorPartitionId int) *PrepareResultOp {
	p := &PrepareResultOp{
		Request:          request,
		CoordPartitionId: coordinatorPartitionId,
	}

	return p
}

type PrintStatusRequestOp struct {
	committedTxn int
	wait         chan bool
}

func NewPrintStatusRequestOp(committedTxn int) *PrintStatusRequestOp {
	p := &PrintStatusRequestOp{
		committedTxn: committedTxn,
		wait:         make(chan bool, 1),
	}

	return p
}

func (o *PrintStatusRequestOp) BlockOwner() bool {
	return <-o.wait
}
