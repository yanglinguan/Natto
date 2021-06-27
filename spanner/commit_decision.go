package spanner

type commitDecision struct {
	commitResult *CommitResult
}

func (o *commitDecision) wait() {
	return
}

func (o *commitDecision) execute(s *Server) {
	// replicate commit decision
	txn := s.txnStore.createTxn(o.commitResult.Id, o.commitResult.Ts, o.commitResult.CId)
	status := ABORTED
	if o.commitResult.Commit {
		status = COMMITTED
	}

	txn.replicate(status, PARTITIONCOMMIT)
}
