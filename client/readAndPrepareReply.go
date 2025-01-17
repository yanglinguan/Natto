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
	if client.txnStore.getCurrentExecutionCount(op.txnId) > op.executionCount {
		logrus.Debugf("txn %v current execution count > op execution count ignore", op.txnId)
		return
	}

	if len(op.reply.KeyValVerList) == 0 {
		return
	}

	execution := client.txnStore.getExecution(op.txnId, op.executionCount)
	execution.onTime = op.reply.OnTime

	for _, kv := range op.reply.KeyValVerList {
		// if one of the keys exist meaning the client
		// is already receive the data from this partition
		// if existed version is newer than we do nothing
		if value, exist := execution.tmpReadResult[kv.Key]; exist && value.Version >= kv.Version {
			//if op.reply.IsLeader {
			//	execution.readFromLeader[kv.Key] = op.reply.IsLeader
			//}
			logrus.Debugf("txn %v already receive key %v and same version",
				op.txnId, kv.Key)
			execution.readFromLeader[kv.Key] = true
			continue
		}
		execution.tmpReadResult[kv.Key] = kv
		//execution.readFromLeader[kv.Key] = op.reply.IsLeader
		execution.readFromLeader[kv.Key] = true
	}

	if !execution.receiveAllReadResult() {
		logrus.Debugf("txn %v received %v expect %v",
			op.reply.TxnId, len(execution.tmpReadResult), execution.readKeyNum)
		return
	} else {
		if client.Config.GetFastPath() && execution.readAndPrepareOp.IsUnBlocked() {
			logrus.Debugf("all ready receive all: txn %v received %v expect %v",
				op.reply.TxnId, len(execution.tmpReadResult), execution.readKeyNum)
			return
		}
	}

	for key, kv := range execution.tmpReadResult {
		execution.readAndPrepareOp.SetKeyValue(key, kv.Value)
		execution.readFromReplica = execution.readFromReplica || !execution.readFromLeader[key]
		execution.readKeyValueVersion = append(execution.readKeyValueVersion, kv)
	}

	if execution.readAndPrepareOp.IsUnBlocked() {
		logrus.Debugf("txn %v resend write data %v", op.reply.TxnId, op.reply.KeyValVerList)
		client.reSendWriteData(op.reply.TxnId, op.reply.KeyValVerList)
	} else {
		logrus.Debugf("txn %v unblock read", op.reply.TxnId)
		execution.readAndPrepareOp.Unblock()
	}
}
