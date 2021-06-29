package spanner

import "github.com/sirupsen/logrus"

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
	go c.start()
	return c
}

func (c *coordinator) addOp(op coordOperation) {
	logrus.Debugf("Add Coord op %v", op.string())
	c.opChan <- op
}

func (c *coordinator) start() {
	for {
		op := <-c.opChan
		logrus.Debugf("Coord op process %v", op.string())
		op.execute(c)
		logrus.Debugf("finish Coord op process %v", op.string())
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
