package client

import (
	"Carousel-GTS/benchmark/workload"
	"Carousel-GTS/configuration"
	"Carousel-GTS/tapir"
	"github.com/sirupsen/logrus"
	"strconv"
	"time"
)

type TapirClient struct {
	clientId int
	lib      *tapir.TapirClient
	config   configuration.Configuration
	txnStore *TxnStore

	operations chan *TapirTxnOp
}

type TapirTxnOp struct {
	txnId       string // clientId-txnId
	tapirClient *TapirClient
	txn         workload.Txn
	isCommitted bool
	wait        chan bool
	isRetry     bool
	waitTime    time.Duration
}

func NewTapirTxnOp(client *TapirClient, txn workload.Txn) *TapirTxnOp {
	t := &TapirTxnOp{
		txnId:       getTxnId(client.clientId, txn.GetTxnId()),
		tapirClient: client,
		txn:         txn,
		isCommitted: false,
		wait:        make(chan bool, 1),
	}
	return t
}

func exec(
	execCount int64,
	execRecord *ExecutionRecord,
	op *TapirTxnOp,
	readSet map[int32][]string,
	writeSet map[int32]map[string]string) {
	_, op.isCommitted, _, _ = op.tapirClient.lib.ExecTxn(readSet, writeSet)
	execRecord.endTime = time.Now()
	logrus.Debugf("txn %v result %v", op.txnId, op.isCommitted)
	if !op.isCommitted {
		execRecord.commitResult = 0
		op.isRetry, op.waitTime = isRetryTxn(execCount+1, op.tapirClient.config)
	} else {
		execRecord.commitResult = 1
	}
	op.wait <- true
}

func (op *TapirTxnOp) Execute() {
	op.tapirClient.txnStore.addTxn(
		nil,
		op.txnId,
		op.txn.GetReadKeys(),
		op.txn.GetWriteKeys(),
		op.txn.GetPriority(),
		op.txn.GetTxnType(),
		op.tapirClient.config)

	readSet := make(map[int32][]string)
	for _, k := range op.txn.GetReadKeys() {
		pId := int32(op.tapirClient.config.GetPartitionIdByKey(k))
		if _, exist := readSet[pId]; !exist {
			readSet[pId] = make([]string, 0)
		}
		readSet[pId] = append(readSet[pId], k)
	}
	writeSet := make(map[int32]map[string]string)
	for _, k := range op.txn.GetWriteKeys() {
		pId := int32(op.tapirClient.config.GetPartitionIdByKey(k))
		if _, exist := writeSet[pId]; !exist {
			writeSet[pId] = make(map[string]string)
		}
		writeSet[pId][k] = k
	}
	execRecord := op.tapirClient.txnStore.getCurrentExecution(op.txnId)
	txn := op.tapirClient.txnStore.getTxn(op.txnId)
	go exec(txn.execCount, execRecord, op, readSet, writeSet)
}

func NewTapirClient(clientId int, config configuration.Configuration) *TapirClient {
	addrTable := make(map[int32][]string)
	for pId, sIds := range config.GetPartitionInfo() {
		pId32 := int32(pId)
		if _, exist := addrTable[pId32]; !exist {
			addrTable[pId32] = make([]string, len(sIds))
		}
		for i, sId := range sIds {
			addrTable[pId32][i] = config.GetServerAddressByServerId(sId)
		}
	}
	t := &TapirClient{
		clientId:   clientId,
		lib:        tapir.NewTapirClient(strconv.Itoa(clientId), 0, addrTable, 0),
		config:     config,
		txnStore:   NewTxnStore(),
		operations: make(chan *TapirTxnOp, config.GetQueueLen()),
	}
	go t.processOperation()
	return t
}

func (t *TapirClient) processOperation() {
	for {
		op := <-t.operations
		op.Execute()
	}
}

func (t *TapirClient) ExecTxn(txn workload.Txn) (bool, bool, time.Duration, time.Duration) {
	op := NewTapirTxnOp(t, txn)
	t.operations <- op
	<-op.wait
	return op.isCommitted, op.isRetry, op.waitTime, 0
}

func (t *TapirClient) Start() {
	t.lib.Start()
}

func (t *TapirClient) Close() {
	t.lib.Shutdown()
	t.txnStore.PrintTxnStatisticData(t.clientId)
}
