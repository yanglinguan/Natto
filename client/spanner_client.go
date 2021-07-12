package client

import (
	"Carousel-GTS/benchmark/workload"
	"Carousel-GTS/configuration"
	"Carousel-GTS/spanner"
	"github.com/sirupsen/logrus"
	"time"
)

type spannerOp struct {
	txn    workload.Txn
	result bool
	wait   chan bool

	isRetry  bool
	waitTime time.Duration
}

func (op *spannerOp) exec(
	execCount int64,
	execRecord *ExecutionRecord,
	client *SpannerClient) {
	// read
	logrus.Debugf("execute txn %v execCount %v", execRecord.rpcTxnId, execRecord)
	readKey := make(map[int][]string)
	for _, key := range op.txn.GetReadKeys() {
		pId := client.config.GetPartitionIdByKey(key)
		if _, exist := readKey[pId]; !exist {
			readKey[pId] = make([]string, 0)
		}
		readKey[pId] = append(readKey[pId], key)
	}

	txnIdToServer := execRecord.rpcTxnId
	txn := client.lib.Begin(txnIdToServer, op.txn.GetPriority())
	aborted, result := client.lib.Read(txn, readKey)
	if aborted {
		op.result = false
		client.lib.Abort(txn)
	} else {
		op.txn.GenWriteData(result)
		writeData := op.txn.GetWriteData()
		writeDataPartitionMap := make(map[int]map[string]string)
		for key, val := range writeData {
			pId := client.config.GetPartitionIdByKey(key)
			if _, exist := writeDataPartitionMap[pId]; !exist {
				writeDataPartitionMap[pId] = make(map[string]string)
			}
			writeDataPartitionMap[pId][key] = val
		}
		op.result = client.lib.Commit(txn, writeDataPartitionMap)
	}
	execRecord.endTime = time.Now()
	logrus.Debugf("txn %v commit result %v", txnIdToServer, op.result)
	if !op.result {
		execRecord.commitResult = 0
		op.isRetry, op.waitTime = isRetryTxn(execCount+1, client.config)
	} else {
		execRecord.commitResult = 1
	}
	op.wait <- true
}

func (op *spannerOp) execute(client *SpannerClient) {
	txnId := getTxnId(client.clientId, op.txn.GetTxnId())
	client.txnStore.addTxn(
		nil,
		txnId,
		op.txn.GetReadKeys(),
		op.txn.GetWriteKeys(),
		op.txn.GetPriority(),
		client.config)

	execRecord := client.txnStore.getCurrentExecution(txnId)
	execCount := client.txnStore.getCurrentExecutionCount(txnId)
	go op.exec(execCount, execRecord, client)
}

type SpannerClient struct {
	clientId int
	lib      *spanner.Client
	config   configuration.Configuration
	txnStore *TxnStore

	operations chan *spannerOp
}

func NewSpannerClient(cId int, config configuration.Configuration) *SpannerClient {
	c := &SpannerClient{
		clientId:   cId,
		lib:        spanner.NewClient(cId, config),
		config:     config,
		txnStore:   NewTxnStore(),
		operations: make(chan *spannerOp, config.GetQueueLen()),
	}
	return c
}

func (c *SpannerClient) processOp() {
	for {
		op := <-c.operations
		op.execute(c)
	}
}

func (c *SpannerClient) Start() {
	go c.processOp()
}

func (c *SpannerClient) Close() {
	c.txnStore.PrintTxnStatisticData(c.clientId)
}

func (c *SpannerClient) ExecTxn(txn workload.Txn) (bool, bool, time.Duration, time.Duration) {

	op := &spannerOp{
		txn:      txn,
		result:   false,
		wait:     make(chan bool),
		isRetry:  false,
		waitTime: 0,
	}

	c.operations <- op
	<-op.wait
	return op.result, op.isRetry, op.waitTime, 0
}
