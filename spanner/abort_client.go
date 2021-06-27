package spanner

type abortClient struct {
	abortRequest *AbortRequest
	waitChan     chan bool
}

func (o *abortClient) wait() {
	<-o.waitChan
}

func (o *abortClient) execute(s *Server) {
	txn := s.txnStore.createTxn(o.abortRequest.Id, o.abortRequest.Ts, o.abortRequest.CId)
	txn.Status = ABORTED
	txn.partitionLeaderCommit()
	o.waitChan <- true
}
