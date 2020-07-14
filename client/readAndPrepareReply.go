package client

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadAndPrepareReply struct {
	reply          *rpc.ReadAndPrepareReply
	executionCount int64
	txnId          string
}

func NewReadAndPrepareReplyOp(txnId string, executionCount int64, reply *rpc.ReadAndPrepareReply) *ReadAndPrepareReply {
	op := &ReadAndPrepareReply{
		txnId:          txnId,
		reply:          reply,
		executionCount: executionCount,
	}
	return op
}

func (op *ReadAndPrepareReply) Execute(client *Client) {
	if client.getCurrentExecutionCount(op.txnId) > op.executionCount {
		logrus.Debugf("txn %v current execution count > op execution count ignore", op.txnId)
		return
	}

	execution := client.getExecution(op.txnId, op.executionCount)

	if execution.receiveAllReadResult() {
		logrus.Debugf("txn %v already receive all read result ignore", op.txnId)
		return
	}

	for _, kv := range op.reply.KeyValVerList {
		if value, exist := execution.tmpReadResult[kv.Key]; exist && value.Version >= kv.Version {
			if op.reply.IsLeader {
				execution.readFromLeader[kv.Key] = op.reply.IsLeader
			}
			continue
		}
		execution.tmpReadResult[kv.Key] = kv
		execution.readFromLeader[kv.Key] = op.reply.IsLeader
	}

	if !execution.receiveAllReadResult() {
		return
	}

	for key, kv := range execution.tmpReadResult {
		execution.readAndPrepareOp.SetKeyValue(key, kv.Value)
		execution.readFromReplica = execution.readFromReplica || !execution.readFromLeader[key]
		execution.readKeyValueVersion = append(execution.readKeyValueVersion, kv)
	}

	execution.readAndPrepareOp.Unblock()
}
