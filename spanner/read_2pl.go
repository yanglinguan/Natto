package spanner

import (
	"fmt"
	"github.com/sirupsen/logrus"
)

type read2PL struct {
	readRequest *ReadRequest
	readResult  []*ValVer
	abort       bool
	waitChan    chan bool
	replied     bool
}

func (o *read2PL) wait() {
	<-o.waitChan
}

func (o *read2PL) string() string {
	return fmt.Sprintf("READ 2PL OP txn %v", o.readRequest.Id)
}

func (o *read2PL) getReadResult() (bool, []*ValVer) {
	return o.abort, o.readResult
}

func (o *read2PL) execute(server *Server) {
	logrus.Debugf("process txn %v read 2pl op", o.readRequest.Id)
	txn := server.txnStore.createTxn(o.readRequest.Id, o.readRequest.Ts, o.readRequest.CId, server)
	txn.setReadKeys(o.readRequest.Keys)
	txn.read2PLOp = o
	// it is possible that txn is already aborted
	if txn.Status == ABORTED {
		o.abort = true
		o.waitChan <- true
		return
	}
	txn.Status = READ
	for _, key := range txn.readKeys {
		txn.keyMap[key] = true
		server.lm.lockShared(txn, key)
	}
	if len(txn.waitKeys) == 0 {
		txn.replyRead()
	}
}
