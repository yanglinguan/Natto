package server

type Executor struct {
	server *Server
	// try to prepare txn
	PrepareTxn chan *ReadAndPrepareOp
	// abort from partition itself or coordinator
	AbortTxn chan *AbortRequestOp
	// commit message from coordinator
	CommitTxn chan *CommitRequestOp

	PrepareResult chan *PrepareResultOp
}

func NewExecutor(server *Server) *Executor {
	e := &Executor{
		server:        server,
		PrepareTxn:    make(chan *ReadAndPrepareOp, QueueLen),
		AbortTxn:      make(chan *AbortRequestOp, QueueLen),
		CommitTxn:     make(chan *CommitRequestOp, QueueLen),
		PrepareResult: make(chan *PrepareResultOp, QueueLen),
	}

	go e.run()
	go e.sendPreparedResultToCoordinator()
	return e
}

func (e *Executor) run() {
	for {
		select {
		case op := <-e.PrepareTxn:
			e.server.storage.Prepare(op)
		case op := <-e.AbortTxn:
			e.server.storage.Abort(op)
		case op := <-e.CommitTxn:
			e.server.storage.Commit(op)
		}
	}
}

func (e *Executor) sendPreparedResultToCoordinator() {
	for {
		op := <-e.PrepareResult
		if op.CoordPartitionId == e.server.partitionId {
			e.server.coordinator.PrepareResult <- op
		} else {
			coordinatorId := e.server.config.GetServerIdByPartitionId(op.CoordPartitionId)
			connection := e.server.connections[coordinatorId]
			sender := &PrepareResultSender{
				request:    op.Request,
				timeout:    10,
				connection: connection,
			}
			go sender.Send()
		}
	}
}
