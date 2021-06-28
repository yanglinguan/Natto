package spanner

type commit2PL struct {
	commitRequest *CommitRequest
	result        bool
	waitChan      chan bool
}

func (o *commit2PL) wait() {
	<-o.waitChan
}

func (o *commit2PL) getCommitResult() bool {
	return o.result
}

func (o *commit2PL) execute(server *Server) {
	txn := server.txnStore.createTxn(o.commitRequest.Id, o.commitRequest.Ts, o.commitRequest.CId)
	txn.commitOp = o
	txn.coordPId = int(o.commitRequest.CoordPId)
	// if txn is already aborted,
	if txn.Status == ABORTED {
		if txn.finalize {
			o.result = false
			o.waitChan <- true
		} else {
			txn.abort()
		}
		return
	}

	writeMap := make(map[string]string)
	for _, kv := range o.commitRequest.WKV {
		txn.keyMap[kv.Key] = true
		writeMap[kv.Key] = kv.Val
	}
	txn.setWriteKeys(writeMap)
	for _, pId := range o.commitRequest.Pp {
		txn.participantPartition[int(pId)] = true
	}

	txn.Status = WRITE
	for key := range writeMap {
		server.lm.lockExclusive(txn, key)
	}
	if len(txn.waitKeys) == 0 {
		txn.prepare()
	}
}
