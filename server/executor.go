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

	PrepareResult     chan *PrepareResultOp
	FastPrepareResult chan *PrepareResultOp

	PrintStatus chan *PrintStatusRequestOp

	ReplicationTxn chan ReplicationMsg

	ReleaseReadOnlyTxn chan *ReadAndPrepareOp

	TimerExpire chan *ReadAndPrepareOp
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
	if server.config.GetFastPath() {
		e.FastPrepareResult = make(chan *PrepareResultOp, queueLen)
		go e.sendFastPrepareResultToCoordinator()
	}
	if server.config.GetIsReadOnly() {
		e.ReleaseReadOnlyTxn = make(chan *ReadAndPrepareOp, queueLen)
	}
	return e
}

func (e *Executor) run() {
	for {
		select {
		case op := <-e.TimerExpire:
			if op.request.Txn.HighPriority {
				e.server.storage.AddHighPriorityTxn(op)
			}
			e.PrepareTxn <- op
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
		case op := <-e.ReleaseReadOnlyTxn:
			e.server.storage.ReleaseReadOnly(op)
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
			logrus.Debugf("txn %v coordPartitionId %v",
				op.Request.TxnId, op.CoordPartitionId)
			coordinatorId := e.server.config.GetLeaderIdByPartitionId(op.CoordPartitionId)
			if coordinatorId == -1 {
				logrus.Fatalf("txn %v coordinatorId is invalid coordPartitionId %v",
					op.Request.TxnId, op.CoordPartitionId)
			}
			sender := NewPrepareResultSender(op.Request, coordinatorId, e.server)
			go sender.Send()
		}
	}
}

func (e *Executor) sendFastPrepareResultToCoordinator() {
	for {
		op := <-e.FastPrepareResult
		logrus.Debugf("send fast prepare result %v to coordinator %v txn %v, isLeader %v, raft term %v ",
			op.Request.PrepareStatus, op.CoordPartitionId, op.Request.TxnId, e.server.IsLeader(), e.server.raftNode.GetRaftTerm())
		request := &rpc.FastPrepareResultRequest{
			PrepareResult: op.Request,
			IsLeader:      e.server.IsLeader(),
			RaftTerm:      e.server.raftNode.GetRaftTerm(),
		}

		fOp := NewFastPrepareRequestOp(request, op.CoordPartitionId)

		if op.CoordPartitionId == e.server.partitionId && e.server.IsLeader() {
			e.server.coordinator.FastPrepareResult <- fOp
		} else {
			coordinatorId := e.server.config.GetLeaderIdByPartitionId(op.CoordPartitionId)
			sender := NewFastPrepareResultSender(request, coordinatorId, e.server)
			go sender.Send()
		}
	}
}
