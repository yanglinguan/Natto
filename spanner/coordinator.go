package spanner

type twoPCInfo struct {
	prepared int
	status   TxnStatus

	commitOp *commitCoord
	txn      *transaction
}

type coordinator struct {
	transactions map[string]*twoPCInfo

	opChan chan coordOperation
}

func newCoordinator(queueLen int) *coordinator {
	c := &coordinator{
		transactions: make(map[string]*twoPCInfo),
		opChan:       make(chan coordOperation, queueLen),
	}
	return c
}

func (c *coordinator) start() {
	for {
		op := <-c.opChan
		op.execute(c)
	}
}

func (c *coordinator) createTwoPCInfo(txn *transaction) *twoPCInfo {
	if _, exist := c.transactions[txn.txnId]; !exist {
		c.transactions[txn.txnId] = &twoPCInfo{
			prepared: 0,
			status:   INIT,
			txn:      txn,
		}
	}
	return c.transactions[txn.txnId]
}

func (c *coordinator) abort(txn *transaction) {
	c.transactions[txn.txnId].status = ABORTED
	txn.replicate(ABORTED, COORDCOMMIT)
}

func (c *coordinator) commit(txn *transaction) {
	c.transactions[txn.txnId].status = COMMITTED
	txn.replicate(COMMITTED, COORDCOMMIT)
}
