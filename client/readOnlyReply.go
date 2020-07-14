package client

import (
	"Carousel-GTS/rpc"
	"Carousel-GTS/server"
	"github.com/sirupsen/logrus"
)

type ReadOnlyReply struct {
	reply          *rpc.ReadAndPrepareReply
	executionCount int64
	txnId          string
}

func NewReadOnlyReplyOp(txnId string, executionCount int64, reply *rpc.ReadAndPrepareReply) *ReadOnlyReply {
	op := &ReadOnlyReply{
		txnId:          txnId,
		reply:          reply,
		executionCount: executionCount,
	}
	return op
}

func (op *ReadOnlyReply) Execute(client *Client) {
	if client.getCurrentExecutionCount(op.txnId) > op.executionCount {
		logrus.Debugf("txn %v current execution count > op execution count ignore", op.txnId)
		return
	}

	execution := client.getExecution(op.txnId, op.executionCount)

	if execution.isAbort || execution.receiveAllReadResult() {
		logrus.Debugf("txn %v abort %v or receive all read result %v", op.txnId,
			execution.isAbort, execution.receiveAllReadResult())
		return
	}

	// wait for result
	//result := make(map[string]*rpc.KeyValueVersion)
	//readLeader := make(map[string]bool)
	//for {
	//	readAndPrepareReply := <-execution.readAndPrepareReply
	execution.isAbort = execution.isAbort || server.TxnStatus(op.reply.Status).IsAbort()
	execution.isConditionalPrepare = execution.isConditionalPrepare ||
		(!execution.isAbort && op.reply.Status == int32(server.CONDITIONAL_PREPARED))
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

	if !execution.isAbort && !execution.receiveAllReadResult() {
		logrus.Debugf("txn %v abort %v or receive all read result %v", op.txnId,
			execution.isAbort, execution.receiveAllReadResult())
		return
	}

	//}
	execution.readAndPrepareOp.SetAbort(execution.isAbort)
	if !execution.isAbort {
		for key, kv := range execution.tmpReadResult {
			execution.readAndPrepareOp.SetKeyValue(key, kv.Value)
			execution.readFromReplica = execution.readFromReplica || !execution.readFromLeader[key]
			execution.readKeyValueVersion = append(execution.readKeyValueVersion, kv)
		}
	}

	execution.readAndPrepareOp.Unblock()
}
