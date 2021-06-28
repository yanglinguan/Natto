package spanner

import "github.com/sirupsen/logrus"

// coord handle the commit request from client
type commitCoord struct {
	commitRequest *CommitRequest
	txn           *transaction
	result        bool
	waitChan      chan bool
}

func (o *commitCoord) wait() {
	<-o.waitChan
}

func (o *commitCoord) getCommitResult() bool {
	return o.result
}

func (o *commitCoord) execute(coordinator *coordinator) {
	twoPCInfo := coordinator.createTwoPCInfo(o.txn)
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
			o.txn.txnId, len(o.commitRequest.Pp))
		return
	}
	logrus.Debugf("txn %v committed", o.txn.txnId)
	coordinator.commit(o.txn)
}
