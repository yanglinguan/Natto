package client

import (
	"Carousel-GTS/rpc"
	"time"
)

type Transaction struct {
	txnId        string
	readKeyList  []string
	writeKeyList []string
	priority     bool
	//commitReply  chan *rpc.CommitReply
	commitResult int
	startTime    time.Time

	execCount  int64
	executions []*ExecutionRecord

	partitionSet           map[int][][]string
	participants           map[int]bool
	participatedPartitions []int32
	serverDcIds            map[int]bool
	serverIdList           []int
}

func NewTransaction(op ReadOp, client *Client) *Transaction {
	t := &Transaction{
		txnId:        op.GetTxnId(),
		readKeyList:  op.GetReadKeyList(),
		writeKeyList: op.GetWriteKeyList(),
		priority:     op.GetPriority(),
		commitResult: 0,
		startTime:    time.Now(),
		//endTime:      time.Time{},
		execCount:  0,
		executions: make([]*ExecutionRecord, 0),
	}

	t.partitionSet, t.participants = client.separatePartition(op)
	t.participatedPartitions, t.serverDcIds, t.serverIdList = client.getParticipantPartition(t.participants)

	return t
}

func (t *Transaction) isReadOnly() bool {
	return len(t.writeKeyList) == 0
}

type ExecutionRecord struct {
	readAndPrepareOp       ReadOp
	commitOp               CommitOp
	rpcTxnId               string
	readKeyNum             int
	coordinatorPartitionId int
	endTime                time.Time
	//rpcTxn               *rpc.Transaction
	//readAndPrepareReply  chan *rpc.ReadAndPrepareReply
	readKeyValueVersion  []*rpc.KeyValueVersion
	isAbort              bool
	isConditionalPrepare bool
	readFromReplica      bool

	tmpReadResult  map[string]*rpc.KeyValueVersion
	readFromLeader map[string]bool
}

func NewExecutionRecord(op ReadOp, rpcTxnId string, readKeyNum int) *ExecutionRecord {
	e := &ExecutionRecord{
		readAndPrepareOp: op,
		rpcTxnId:         rpcTxnId,
		readKeyNum:       readKeyNum,
		//rpcTxn:              rpcTxn,
		//readAndPrepareReply: make(chan *rpc.ReadAndPrepareReply, len(rpcTxn.ParticipatedPartitionIds)),
		readKeyValueVersion:  make([]*rpc.KeyValueVersion, 0),
		isAbort:              false,
		isConditionalPrepare: false,
		readFromReplica:      false,
		tmpReadResult:        make(map[string]*rpc.KeyValueVersion),
		readFromLeader:       make(map[string]bool),
	}
	return e
}

func (e *ExecutionRecord) receiveAllReadResult() bool {
	return len(e.tmpReadResult) == e.readKeyNum
}

func (e *ExecutionRecord) setCoordinatorPartitionId(pId int) {
	e.coordinatorPartitionId = pId
}

func (e *ExecutionRecord) setCommitOp(c CommitOp) {
	e.commitOp = c
}
