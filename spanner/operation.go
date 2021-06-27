package spanner

type operation interface {
	execute(server *Server)
	wait()
}

type readOp interface {
	operation
	getReadResult() (bool, []*ValVer)
}

type commitOp interface {
	operation
	getCommitResult() bool
}

type coordOperation interface {
	execute(coord *coordinator)
	wait()
}

type opCreator interface {
	createReadOp(request *ReadRequest) readOp
	createCommitOp(request *CommitRequest) commitOp
}

type twoPLOpCreator struct {
}

func (t *twoPLOpCreator) createReadOp(request *ReadRequest) readOp {
	op := &read2PL{
		readRequest: request,
		readResult:  nil,
		waitChan:    make(chan bool),
	}
	return op
}

func (t *twoPLOpCreator) createCommitOp(request *CommitRequest) commitOp {
	op := &commit2PL{
		commitRequest: request,
		result:        false,
		waitChan:      make(chan bool),
	}
	return op
}
