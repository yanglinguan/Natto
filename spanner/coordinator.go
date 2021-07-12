package spanner

import (
	"context"
	"github.com/sirupsen/logrus"
)

type twoPCInfo struct {
	prepared int
	status   TxnStatus

	commitOp    *commitCoord
	txn         *transaction
	replyClient bool
}

type coordinator struct {
	transactions map[string]*twoPCInfo
	server       *Server
	opChan       chan coordOperation
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

func (c *coordinator) createTwoPCInfo(txnId string, ts int64, cId int64, priority bool) *twoPCInfo {
	if _, exist := c.transactions[txnId]; !exist {
		txn := NewTransaction(txnId, ts, cId, priority)
		txn.server = c.server
		c.transactions[txn.txnId] = &twoPCInfo{
			prepared: 0,
			status:   INIT,
			txn:      txn,
		}
	}
	return c.transactions[txnId]
}

func (c *coordinator) abort(txn *transaction) {
	c.transactions[txn.txnId].status = ABORTED
	txn.replicate(ABORTED, COORDCOMMIT)
}

func (c *coordinator) commit(txn *transaction) {
	c.transactions[txn.txnId].status = COMMITTED
	txn.replicate(COMMITTED, COORDCOMMIT)
}

func (c *coordinator) sendCommitDecision(txnId string, result bool, pId int) {
	commitResult := &CommitResult{
		Commit: result,
		Id:     txnId,
	}

	// send to participant partition
	leaderId := c.server.config.GetLeaderIdByPartitionId(pId)

	logrus.Debugf("txn %v send commit decision to server %v pId %v",
		txnId, leaderId, pId)
	conn := c.server.connection[leaderId]
	client := NewSpannerClient(conn.GetConn())

	_, err := client.CommitDecision(context.Background(), commitResult)
	if err != nil {
		logrus.Fatalf("txn %v coord cannot sent commit decision to partition %v",
			txnId, pId)
	}
}

func (c *coordinator) applyCoordCommit(message ReplicateMessage) {
	logrus.Debugf("txn %v replicated coord commit status %v", message.TxnId, message.Status)
	twoPCInfo := c.createTwoPCInfo(message.TxnId, message.Timestamp, message.ClientId, message.Priority)
	twoPCInfo.status = message.Status
	txn := twoPCInfo.txn
	if c.server.IsLeader() {
		if !twoPCInfo.replyClient && twoPCInfo.commitOp != nil {
			twoPCInfo.replyClient = true
			twoPCInfo.commitOp.result = twoPCInfo.status == COMMITTED
			twoPCInfo.commitOp.waitChan <- true
		}

		for pid := range txn.participantPartition {
			if c.server.pId == pid {
				message.MsgType = PARTITIONCOMMIT
				op := &replicateResultOp{replicationMsg: message}
				c.server.addOp(op)
				continue
			}
			// send the commit decision to partition leader
			go c.sendCommitDecision(txn.txnId, twoPCInfo.commitOp.result, pid)
		}
	}
}
