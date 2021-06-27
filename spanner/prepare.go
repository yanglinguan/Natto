package spanner

import "github.com/sirupsen/logrus"

// coordinator handle the prepare result from the partition leaders
type prepare struct {
	prepareRequest *PrepareRequest
	txn            *transaction
}

func newPrepare(pRequest *PrepareRequest, txn *transaction) *prepare {
	p := &prepare{
		prepareRequest: pRequest,
		txn:            txn,
	}
	return p
}

func (o *prepare) wait() {
	return
}

func (o *prepare) execute(coordinator *coordinator) {
	twoPCInfo := coordinator.createTwoPCInfo(o.txn)
	if twoPCInfo.status == ABORTED {
		logrus.Debugf("txn %v already aborted", o.prepareRequest.Id)
		return
	}

	if !o.prepareRequest.Prepared {
		logrus.Debugf("txn %v partition %v aborted", o.txn.txnId, o.prepareRequest.PId)
		coordinator.abort(o.txn)
		return
	}

	twoPCInfo.prepared++
	logrus.Debugf("txn %v partition %v prepared", o.txn.txnId, o.prepareRequest.PId)
	if twoPCInfo.commitOp != nil {
		logrus.Debugf("txn %v receive %v prepared, require %v",
			twoPCInfo.prepared, len(o.txn.participantPartition))
		if twoPCInfo.prepared == len(o.txn.participantPartition) {
			logrus.Debugf("txn %v committed", o.txn.txnId)
			coordinator.commit(o.txn)
		}
	}
}
