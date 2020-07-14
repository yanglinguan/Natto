package client

import (
	"github.com/sirupsen/logrus"
	"time"
)

type CommitReadOnly struct {
	*Commit
}

func NewCommitReadOnlyOp(txnId string) *CommitReadOnly {
	op := &CommitReadOnly{NewCommitOp(txnId, nil)}
	return op
}

func (op *CommitReadOnly) Execute(client *Client) {
	ongoingTxn := client.getTxn(op.txnId)
	execution := client.getCurrentExecution(op.txnId)
	execution.endTime = time.Now()
	logrus.Debugf("read only txn %v commit", op.txnId)
	ongoingTxn.commitResult = 1
	op.result = true
	if client.Config.GetTargetRate() > 0 {
		latency := execution.endTime.Sub(ongoingTxn.startTime)
		op.expectWait = client.tryToMaintainTxnTargetRate(latency)
	}
	op.Unblock()
}
