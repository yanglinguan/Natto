package spanner

// handle the client abort request
type abortClient struct {
	abortRequest *AbortRequest
	waitChan     chan bool
}

func (o *abortClient) wait() {
	<-o.waitChan
}

// client send abort when the read request is fail
func (o *abortClient) execute(s *Server) {
	txn := s.txnStore.createTxn(o.abortRequest.Id, o.abortRequest.Ts, o.abortRequest.CId)
	txn.Status = ABORTED
	txn.partitionLeaderCommit()
	o.waitChan <- true
}
