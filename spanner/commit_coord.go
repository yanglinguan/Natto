package spanner

import (
	"fmt"
	"github.com/sirupsen/logrus"
)

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

func (o *commitCoord) string() string {
	return fmt.Sprintf("COMMIT COORD OP txn %v", o.commitRequest.Id)
}

func (o *commitCoord) getCommitResult() bool {
	return o.result
}

func (o *commitCoord) execute(coordinator *coordinator) {

	twoPCInfo := coordinator.createTwoPCInfo(
		o.commitRequest.Id, o.commitRequest.Ts, o.commitRequest.CId)
	txn := twoPCInfo.txn
	for _, pId := range o.commitRequest.Pp {
		txn.participantPartition[int(pId)] = true
	}
	twoPCInfo.commitOp = o
	status := twoPCInfo.status
	if status == ABORTED {
		twoPCInfo.replyClient = true
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
