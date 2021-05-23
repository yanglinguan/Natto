package client

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type ReadReplyFromCoordinator struct {
	result *rpc.ReadReplyFromCoordinator
}

func NewReadReplyFromCoordinatorOp(reply *rpc.ReadReplyFromCoordinator) *ReadReplyFromCoordinator {
	r := &ReadReplyFromCoordinator{result: reply}
	return r
}

func (op *ReadReplyFromCoordinator) Execute(client *Client) {
	count := client.getExecutionCountByTxnId(op.result.TxnId)
	clientTxnId := client.getTxnIdByServerTxnId(op.result.TxnId)
	if client.txnStore.getCurrentExecutionCount(clientTxnId) != count {
		logrus.Debugf("txn %v current execution count != op execution count ignore",
			op.result.TxnId)
		return
	}
	execution := client.txnStore.getCurrentExecution(clientTxnId)
	if execution.receiveAllReadResult() {
		logrus.Debugf("txn %v already receive all read result", op.result.TxnId)
		return
	}
	for _, kv := range op.result.KeyValVerList {
		if _, exist := execution.tmpReadResult[kv.Key]; exist {
			logrus.Debugf("txn %v key %v already receive from leader", op.result.TxnId, kv.Key)
			return
		}
		execution.readFromLeader[kv.Key] = false
		execution.tmpReadResult[kv.Key] = kv
	}
	if !execution.receiveAllReadResult() {
		return
	}

	for key, kv := range execution.tmpReadResult {
		execution.readAndPrepareOp.SetKeyValue(key, kv.Value)
		execution.readFromReplica = execution.readFromReplica || !execution.readFromLeader[key]
		execution.readKeyValueVersion = append(execution.readKeyValueVersion, kv)
	}

	if execution.readAndPrepareOp.IsUnBlocked() {
		// re-commit
		logrus.Debugf("txn %v this is read from coordinator do not resend write data", op.result.TxnId)
		//if client.Config.ForwardReadToCoord() {
		//	client.reSendWriteData(op.result.TxnId, op.result.KeyValVerList)
		//}
	} else {
		execution.readAndPrepareOp.Unblock()
	}
}
