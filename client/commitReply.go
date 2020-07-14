package client

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/rpc"
	"time"
)

type CommitReply struct {
	reply *rpc.CommitReply
	txnId string
}

func NewCommitReplyOp(txnId string, reply *rpc.CommitReply) *CommitReply {
	op := &CommitReply{
		reply: reply,
		txnId: txnId,
	}

	return op
}

func (op *CommitReply) Execute(client *Client) {
	//result := <-ongoingTxn.commitReply
	ongoingTxn := client.txnStore[op.txnId]

	execution := client.getCurrentExecution(op.txnId)
	execution.endTime = time.Now()

	latency := execution.endTime.Sub(ongoingTxn.startTime)
	isRetry := false
	waitTime := time.Duration(0)
	expWait := time.Duration(0)
	if op.reply.Result {
		ongoingTxn.commitResult = 1
		//ongoingTxn.fastPrepare = result.FastPrepare
	} else {
		ongoingTxn.commitResult = 0
		isRetry, waitTime = client.isRetryTxn(ongoingTxn.execCount + 1)
	}
	//op.result = op.reply.Result
	if client.Config.GetTargetRate() > 0 {
		if op.reply.Result || client.Config.GetRetryMode() == configuration.OFF {
			expWait = client.tryToMaintainTxnTargetRate(latency)
		}
	}
	execution.commitOp.SetResult(op.reply.Result, isRetry, waitTime, expWait)
	execution.commitOp.Unblock()
}
