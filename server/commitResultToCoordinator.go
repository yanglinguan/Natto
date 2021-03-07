package server

import "Carousel-GTS/rpc"

// for forward prepare
type CommitResultToCoordinator struct {
	request *rpc.CommitResult
}

func NewCommitResultToCoordinator(request *rpc.CommitResult) *CommitResultToCoordinator {
	c := &CommitResultToCoordinator{request: request}
	return c
}

func (op *CommitResultToCoordinator) Execute(c *Coordinator) {
	twoPCInfo := c.initTwoPCInfoIfNotExist(op.request.TxnId)
	if op.request.Result {
		twoPCInfo.status = COMMIT
	} else {
		twoPCInfo.status = FORWARD_ABORT
	}

	for waitingTxn := range twoPCInfo.waitingTxns {
		c.checkResult(c.initTwoPCInfoIfNotExist(waitingTxn))
	}
}
