package client

import (
	"Carousel-GTS/rpc"
	"time"
)

type Commit struct {
	txnId         string
	writeKeyValue map[string]string
	wait          chan bool
	result        bool
	retry         bool
	waitTime      time.Duration
	expectWait    time.Duration // try to keep the target rate
}

func NewCommitOp(txnId string, writeKeyValue map[string]string) *Commit {
	op := &Commit{
		txnId:         txnId,
		writeKeyValue: writeKeyValue,
		wait:          make(chan bool, 1),
		result:        false,
		retry:         false,
		waitTime:      0,
		expectWait:    0,
	}

	return op
}

func (op *Commit) Execute(client *Client) {
	//ongoingTxn := client.txnStore[op.txnId]
	execution := client.getCurrentExecution(op.txnId)

	writeKeyValueList := make([]*rpc.KeyValue, len(op.writeKeyValue))
	i := 0
	for k, v := range op.writeKeyValue {
		writeKeyValueList[i] = &rpc.KeyValue{
			Key:   k,
			Value: v,
		}
		i++
	}
	readKeyVerList := make([]*rpc.KeyVersion, 0)
	// if all keys read from leader, we do not need to send read version to coordinator
	if execution.readFromReplica {
		readKeyVerList := make([]*rpc.KeyVersion, len(execution.readKeyValueVersion))
		i = 0
		for _, kv := range execution.readKeyValueVersion {
			readKeyVerList[i] = &rpc.KeyVersion{
				Key:     kv.Key,
				Version: kv.Version,
			}
			i++
		}
	}

	request := &rpc.CommitRequest{
		TxnId:            execution.rpcTxnId,
		WriteKeyValList:  writeKeyValueList,
		FromCoordinator:  false,
		ReadKeyVerList:   readKeyVerList,
		IsReadAnyReplica: execution.readFromReplica,
	}

	coordinatorId := client.Config.GetLeaderIdByPartitionId(execution.coordinatorPartitionId)
	sender := NewCommitRequestSender(request, op.txnId, coordinatorId, client)

	go sender.Send()
}

func (op *Commit) Block() {
	<-op.wait
}

func (op *Commit) Unblock() {
	op.wait <- true
}

func (op *Commit) GetResult() (bool, bool, time.Duration, time.Duration) {
	return op.result, op.retry, op.waitTime, op.expectWait
}

func (op *Commit) SetResult(result bool, isRetry bool, waitTime time.Duration, expWait time.Duration) {
	op.result = result
	op.retry = isRetry
	op.waitTime = waitTime
	op.expectWait = expWait
}
