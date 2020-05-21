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
	readKeyMap map[string]bool
	//preparedReadKeyNum  int
	writeKeyMap map[string]bool
	//preparedWriteKeyNum int

	otherPartitionReadKey  []string
	otherPartitionWriteKey []string

	keyMap map[string]bool

	allKeys map[string]bool

	partitionKeys map[int]map[string]bool

	//// prepare result will send to coordinator
	//prepareResult *rpc.PrepareResultRequest

	sendToCoordinator bool

	passedTimestamp bool
	txnId           string

	selfAbort    bool // true: there is a conflict high priority txn within
	allReadKeys  map[string]bool
	allWriteKeys map[string]bool
}

func NewReadAndPrepareOpWithReplicatedMsg(msg ReplicationMsg, server *Server) *ReadAndPrepareOp {
	r := &ReadAndPrepareOp{
		request:       nil,
		wait:          nil,
		index:         0,
		reply:         nil,
		readKeyMap:    make(map[string]bool),
		writeKeyMap:   make(map[string]bool),
		keyMap:        make(map[string]bool),
		allKeys:       make(map[string]bool),
		partitionKeys: make(map[int]map[string]bool),
		//prepareResult:     nil,
		sendToCoordinator: false,
		passedTimestamp:   false,
		txnId:             msg.TxnId,
	}
	readKeyList := make([]string, len(msg.PreparedReadKeyVersion))
	for i, kv := range msg.PreparedReadKeyVersion {
		readKeyList[i] = kv.Key
	}
	writeKeyList := make([]string, len(msg.PreparedWriteKeyVersion))
	for i, kv := range msg.PreparedWriteKeyVersion {
		writeKeyList[i] = kv.Key
	}
	r.processKey(readKeyList, server, READ)
	r.processKey(writeKeyList, server, WRITE)

	return r
}

func NewReadAndPrepareOp(request *rpc.ReadAndPrepareRequest, server *Server) *ReadAndPrepareOp {
	r := &ReadAndPrepareOp{
		request:     request,
		wait:        make(chan bool, 1),
		index:       0,
		reply:       nil,
		readKeyMap:  make(map[string]bool),
		writeKeyMap: make(map[string]bool),
		keyMap:      make(map[string]bool),
		//prepareResult:     nil,
		sendToCoordinator:      false,
		partitionKeys:          make(map[int]map[string]bool),
		allKeys:                make(map[string]bool),
		otherPartitionReadKey:  make([]string, 0),
		otherPartitionWriteKey: make([]string, 0),
		passedTimestamp:        false,
		txnId:                  request.Txn.TxnId,
		allReadKeys:            make(map[string]bool),
		allWriteKeys:           make(map[string]bool),
	}

	r.processKey(request.Txn.ReadKeyList, server, READ)
	r.processKey(request.Txn.WriteKeyList, server, WRITE)

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
		if keyType == WRITE {
			o.allWriteKeys[key] = false
		} else if keyType == READ {
			o.allReadKeys[key] = false
		}

		if !server.storage.HasKey(key) {
			if keyType == WRITE {
				o.otherPartitionWriteKey = append(o.otherPartitionWriteKey, key)
			} else if keyType == READ {
				o.otherPartitionReadKey = append(o.otherPartitionReadKey, key)
			}
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
	abortRequest *rpc.AbortRequest
}

func NewAbortRequestOp(abortRequest *rpc.AbortRequest) *AbortRequestOp {
	a := &AbortRequestOp{
		abortRequest: abortRequest,
	}
	return a
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

func NewPrepareRequestOpWithReplicatedMsg(partitionId int, msg ReplicationMsg) *PrepareResultOp {
	request := &rpc.PrepareResultRequest{
		TxnId:           msg.TxnId,
		ReadKeyVerList:  msg.PreparedReadKeyVersion,
		WriteKeyVerList: msg.PreparedWriteKeyVersion,
		PartitionId:     int32(partitionId),
		PrepareStatus:   int32(msg.Status),
	}
	op := &PrepareResultOp{
		Request:          request,
		CoordPartitionId: -1,
	}

	return op
}

type FastPrepareResultOp struct {
	request          *rpc.FastPrepareResultRequest
	coordPartitionId int
}

func NewFastPrepareRequestOp(request *rpc.FastPrepareResultRequest, coordinatorPartitionId int) *FastPrepareResultOp {
	p := &FastPrepareResultOp{
		request:          request,
		coordPartitionId: coordinatorPartitionId,
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
