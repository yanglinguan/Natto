package spanner

import "fmt"

// partition leader handles the commit decision from coordinator
type commitDecision struct {
	commitResult *CommitResult
}

func (o *commitDecision) wait() {
	return
}

func (o *commitDecision) string() string {
	return fmt.Sprintf("COMMIT DECISION OP txn %v", o.commitResult.Id)
}

func (o *commitDecision) execute(s *Server) {
	// replicate commit decision
	txn := s.txnStore.createTxn(o.commitResult.Id, o.commitResult.Ts, o.commitResult.CId, s)
	txn.finalize = true
	status := ABORTED
	if o.commitResult.Commit {
		status = COMMITTED
	}
	txn.Status = status
	txn.replicate(status, PARTITIONCOMMIT)
}
