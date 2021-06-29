package spanner

import (
	"fmt"
	"github.com/sirupsen/logrus"
)

// coordinator handle the prepare result from the partition leaders
type prepare struct {
	prepareRequest *PrepareRequest
}

func (o *prepare) wait() {
	return
}

func (o *prepare) string() string {
	return fmt.Sprintf("PREPARE OP txn %v pid %v", o.prepareRequest.Id, o.prepareRequest.PId)
}

func (o *prepare) execute(coordinator *coordinator) {
	twoPCInfo := coordinator.createTwoPCInfo(
		o.prepareRequest.Id, o.prepareRequest.Ts, o.prepareRequest.CId)
	txn := twoPCInfo.txn
	if twoPCInfo.status == ABORTED {
		logrus.Debugf("txn %v already aborted", o.prepareRequest.Id)
		return
	}

	if !o.prepareRequest.Prepared {
		logrus.Debugf("txn %v partition %v aborted", txn.txnId, o.prepareRequest.PId)
		coordinator.abort(txn)
		return
	}

	twoPCInfo.prepared++
	logrus.Debugf("txn %v partition %v prepared", txn.txnId, o.prepareRequest.PId)
	if twoPCInfo.commitOp != nil {
		logrus.Debugf("txn %v receive %v prepared, require %v",
			txn.txnId, twoPCInfo.prepared, len(txn.participantPartition))
		if twoPCInfo.prepared == len(txn.participantPartition) {
			logrus.Debugf("txn %v committed", txn.txnId)
			coordinator.commit(txn)
		}
	} else {
		logrus.Debugf("txn %v dose not receives commit msg from client", txn.txnId)
	}
}
