package spanner

type read2PL struct {
	readRequest *ReadRequest
	readResult  []*ValVer
	abort       bool
	waitChan    chan bool
}

func (o *read2PL) wait() {
	<-o.waitChan
}

func (o *read2PL) getReadResult() (bool, []*ValVer) {
	return o.abort, o.readResult
}

func (o *read2PL) execute(server *Server) {
	txn := server.txnStore.createTxn(o.readRequest.Id, o.readRequest.Ts, o.readRequest.CId)
	txn.setReadKeys(o.readRequest.Keys)
	txn.read2PLOp = o
	txn.Status = READ
	for _, key := range txn.readKeys {
		txn.keyMap[key] = true
		server.lm.lockShared(txn, key)
	}
	if len(txn.waitKeys) == 0 {
		txn.replyRead()
	}
}
