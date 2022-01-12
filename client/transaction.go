package client

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/rpc"
	"Carousel-GTS/utils"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"time"
)

type TxnStore struct {
	store map[string]*Transaction
}

func NewTxnStore() *TxnStore {
	t := &TxnStore{store: make(map[string]*Transaction)}
	return t
}

func (t *TxnStore) addTxn(
	op ReadOp,
	txnId string,
	readKeyList []string,
	writeKeyList []string,
	priority bool,
	txnType string,
	config configuration.Configuration) {
	//txnId := op.GetTxnId()

	if _, exist := t.store[txnId]; exist {
		// if exist increment the execution number
		t.store[txnId].execCount++
		//op.ClearReadKeyList()
		//op.ClearWriteKeyList()
		logrus.Debugf("RETRY txn %v: %v", txnId, t.store[txnId].execCount)
	} else {
		// otherwise add new txn
		logrus.Debugf("Add NEW txn %v", txnId)
		t.store[txnId] = NewTransaction(
			txnId, readKeyList, writeKeyList, priority, txnType, config)
	}

	rpcTxnId := t.genTxnIdToServer(txnId)
	execution := NewExecutionRecord(op, rpcTxnId, len(readKeyList))

	logrus.Debugf("txn %v added, keys: %v",
		rpcTxnId, t.store[txnId].partitionSet)

	t.store[txnId].executions = append(t.store[txnId].executions, execution)
}

func (t *TxnStore) genTxnIdToServer(txnId string) string {
	//c.count++
	tId := fmt.Sprintf("%v-%v", txnId, t.store[txnId].execCount)
	//return "c" + strconv.Itoa(c.clientId) + "-" + strconv.Itoa(c.count)
	return tId
}

func (t *TxnStore) getCurrentExecutionTxnId(txnId string) string {
	exec := t.store[txnId].execCount
	return t.store[txnId].executions[exec].rpcTxnId
}

func (t *TxnStore) getCurrentExecutionCount(txnId string) int64 {
	return t.store[txnId].execCount
}

func (t *TxnStore) getCurrentExecution(txnId string) *ExecutionRecord {
	currentCont := t.store[txnId].execCount
	return t.store[txnId].executions[currentCont]
}

func (t *TxnStore) getExecution(txnId string, count int64) *ExecutionRecord {
	return t.store[txnId].executions[count]
}

func (t *TxnStore) getTxn(txnId string) *Transaction {
	return t.store[txnId]
}

func (t *TxnStore) PrintTxnStatisticData(clientId int) {
	file, err := os.Create("c" + strconv.Itoa(clientId) + ".statistic")
	if err != nil || file == nil {
		logrus.Fatal("Fails to create log file: statistic.log")
		return
	}

	_, err = file.WriteString("#txnId, commit result, latency, start time, end time, keys\n")
	if err != nil {
		logrus.Fatalf("Cannot write to file, %v", err)
		return
	}

	for _, txn := range t.store {
		key := make(map[int64]bool)
		for _, ks := range txn.readKeyList {
			k := utils.ConvertToInt(ks)
			key[k] = true
		}

		for _, ks := range txn.writeKeyList {
			k := utils.ConvertToInt(ks)
			key[k] = true
		}
		keyList := make([]int64, len(key))
		i := 0
		for k := range key {
			keyList[i] = k
			i++
		}

		passTimestampAbort := 0
		passTimestampTxn := 0
		for _, exec := range txn.executions {
			if !exec.onTime {
				passTimestampTxn++
				if exec.commitResult == 0 {
					passTimestampAbort++
				}
			}
		}

		s := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n",
			txn.txnId, // 0
			txn.executions[txn.execCount].commitResult,                             // 1
			txn.executions[txn.execCount].endTime.Sub(txn.startTime).Nanoseconds(), // 2
			txn.startTime.UnixNano(),                                               // 3
			txn.executions[txn.execCount].endTime.UnixNano(),                       // 4
			keyList,            // 5
			txn.execCount,      // 6
			txn.isReadOnly(),   // 7
			txn.priority,       // 8
			passTimestampTxn,   // 9
			passTimestampAbort, // 10
			txn.txnType,
		)
		_, err = file.WriteString(s)
		if err != nil {
			logrus.Fatalf("Cannot write to file %v", err)
		}
	}

	err = file.Close()
	if err != nil {
		logrus.Fatalf("cannot close file %v", err)
	}
}

type Transaction struct {
	txnId        string
	readKeyList  []string
	writeKeyList []string
	priority     bool
	//commitReply  chan *rpc.CommitReply
	//commitResult int
	startTime time.Time

	execCount  int64
	executions []*ExecutionRecord

	partitionSet           map[int][][]string
	participants           map[int]bool
	participatedPartitions []int32
	serverDcIds            map[int]bool
	serverIdList           []int
	txnType                string
}

func NewTransaction(
	txnId string,
	readKeyList []string,
	writeKeyList []string,
	priority bool,
	txnType string,
	config configuration.Configuration) *Transaction {
	t := &Transaction{
		txnId:        txnId,
		readKeyList:  readKeyList,
		writeKeyList: writeKeyList,
		priority:     priority,
		//commitResult: 0,
		startTime: time.Now(),
		//endTime:      time.Time{},
		execCount:  0,
		executions: make([]*ExecutionRecord, 0),
		txnType:    txnType,
	}

	t.partitionSet, t.participants = separatePartition(readKeyList, writeKeyList, config)
	t.participatedPartitions, t.serverDcIds, t.serverIdList = getParticipantPartition(t.participants, config)

	return t
}

func (t *Transaction) isReadOnly() bool {
	return len(t.writeKeyList) == 0
}

type ExecutionRecord struct {
	readAndPrepareOp       ReadOp
	commitOp               *Commit
	rpcTxnId               string
	readKeyNum             int
	coordinatorPartitionId int
	endTime                time.Time
	commitResult           int
	//rpcTxn               *rpc.Transaction
	//readAndPrepareReply  chan *rpc.ReadAndPrepareReply
	readKeyValueVersion  []*rpc.KeyValueVersion
	isAbort              bool
	isConditionalPrepare bool
	readFromReplica      bool

	tmpReadResult  map[string]*rpc.KeyValueVersion
	readFromLeader map[string]bool
	onTime         bool
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

func (e *ExecutionRecord) setCommitOp(c *Commit) {
	e.commitOp = c
}
