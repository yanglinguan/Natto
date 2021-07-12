package spanner

import "fmt"

// handle the client abort request
type abortClient struct {
	abortRequest *AbortRequest
	waitChan     chan bool
}

func (o *abortClient) wait() {
	<-o.waitChan
}

func (o *abortClient) string() string {
	return fmt.Sprintf("ABORT CLIENT OP txn %v", o.abortRequest.Id)
}

// client send abort when the read request is fail
func (o *abortClient) execute(s *Server) {
	txn := s.txnStore.createTxn(o.abortRequest.Id, o.abortRequest.Ts, o.abortRequest.CId, o.abortRequest.P, s)
	txn.Status = ABORTED
	txn.partitionLeaderCommit()
	o.waitChan <- true
}
