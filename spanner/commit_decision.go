package spanner

// partition leader handles the commit decision from coordinator
type commitDecision struct {
	commitResult *CommitResult
}

func (o *commitDecision) wait() {
	return
}

func (o *commitDecision) execute(s *Server) {
	// replicate commit decision
	txn := s.txnStore.createTxn(o.commitResult.Id, o.commitResult.Ts, o.commitResult.CId, s)
	txn.finalize = true
	status := ABORTED
	if o.commitResult.Commit {
		status = COMMITTED
	}

	txn.replicate(status, PARTITIONCOMMIT)
}
