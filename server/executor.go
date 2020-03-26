package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
)

type Executor struct {
	server *Server
	// try to prepare txn
	PrepareTxn chan *ReadAndPrepareOp
	// abort from partition itself or coordinator
	AbortTxn chan *AbortRequestOp
	// commit message from coordinator
	CommitTxn chan *CommitRequestOp

	PrepareResult chan *PrepareResultOp

	PrintStatus chan *PrintStatusRequestOp

	ReplicationTxn chan ReplicationMsg
}

func NewExecutor(server *Server) *Executor {
	queueLen := server.config.GetQueueLen()
	e := &Executor{
		server:         server,
		PrepareTxn:     make(chan *ReadAndPrepareOp, queueLen),
		AbortTxn:       make(chan *AbortRequestOp, queueLen),
		CommitTxn:      make(chan *CommitRequestOp, queueLen),
		PrepareResult:  make(chan *PrepareResultOp, queueLen),
		PrintStatus:    make(chan *PrintStatusRequestOp, 1),
		ReplicationTxn: make(chan ReplicationMsg, queueLen),
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
		case op := <-e.PrintStatus:
			e.server.storage.PrintStatus(op)
		case msg := <-e.ReplicationTxn:
			e.server.storage.ApplyReplicationMsg(msg)
		}
	}
}

func (e *Executor) sendPreparedResultToCoordinator() {
	for {
		op := <-e.PrepareResult
		logrus.Debugf("send prepare result %v to coordinator %v txn %v",
			op.Request.PrepareStatus, op.CoordPartitionId, op.Request.TxnId)
		if op.CoordPartitionId == e.server.partitionId {
			e.server.coordinator.PrepareResult <- op
		} else {
			coordinatorId := e.server.config.GetLeaderIdByPartitionId(op.CoordPartitionId)
			sender := NewPrepareResultSender(op.Request, coordinatorId, e.server)
			go sender.Send()
		}
	}
}

func (e *Executor) sendFastPrepareResultToCoordinator() {
	for {
		op := <-e.PrepareResult
		logrus.Debugf("send fast prepare result %v to coordinator %v txn %v, isLeader %v, raft term %v ",
			op.Request.PrepareStatus, op.CoordPartitionId, op.Request.TxnId, e.server.IsLeader(), e.server.raftNode.GetRaftTerm())
		request := &rpc.FastPrepareResultRequest{
			PrepareResult: op.Request,
			IsLeader:      e.server.IsLeader(),
			RaftTerm:      e.server.raftNode.GetRaftTerm(),
		}

		fOp := NewFastPrepareRequestOp(request, op.CoordPartitionId)

		if op.CoordPartitionId == e.server.partitionId {
			e.server.coordinator.FastPrepareResult <- fOp
		} else {
			coordinatorId := e.server.config.GetLeaderIdByPartitionId(op.CoordPartitionId)
			sender := NewFastPrepareResultSender(request, coordinatorId, e.server)
			go sender.Send()
		}
	}
}
