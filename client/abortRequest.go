package client

import "time"

type Abort struct {
	txnId    string
	isRetry  bool
	waitTime time.Duration

	wait chan bool
}

func NewAbortOp(txnId string) *Abort {
	op := &Abort{
		txnId:    txnId,
		isRetry:  false,
		waitTime: 0,
		wait:     make(chan bool, 1),
	}
	return op
}

func (op *Abort) Execute(client *Client) {
	execCount := client.txnStore[op.txnId].execCount
	op.isRetry, op.waitTime = client.isRetryTxn(execCount + 1)
	op.Unblock()
	//return c.isRetryTxn(execCount + 1)
}

func (op *Abort) Block() {
	<-op.wait
}

func (op *Abort) Unblock() {
	op.wait <- true
}
