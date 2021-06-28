package spanner

import "github.com/sirupsen/logrus"

// coord handle the commit request from client
type commitCoord struct {
	commitRequest *CommitRequest
	result        bool
	waitChan      chan bool
	server        *Server
}

func (o *commitCoord) wait() {
	<-o.waitChan
}

func (o *commitCoord) getCommitResult() bool {
	return o.result
}

func (o *commitCoord) execute(coordinator *coordinator) {
	txn := o.server.txnStore.createTxn(o.commitRequest.Id, o.commitRequest.Ts, o.commitRequest.CId, o.server)
	for _, pId := range o.commitRequest.Pp {
		txn.participantPartition[int(pId)] = true
	}
	twoPCInfo := coordinator.createTwoPCInfo(txn)
	twoPCInfo.commitOp = o
	status := twoPCInfo.status
	if status == ABORTED {
		o.result = false
		o.waitChan <- true
		logrus.Debugf("txn %v is already aborted", o.commitRequest.Id)
		return
	}

	// check if receives the prepared messages from the participant partitions
	if twoPCInfo.prepared != len(o.commitRequest.Pp) {
		logrus.Debugf("txn %v receives %v prepared required %v",
			txn.txnId, twoPCInfo.prepared, len(o.commitRequest.Pp))
		return
	}
	logrus.Debugf("txn %v committed", txn.txnId)
	coordinator.commit(txn)
}
