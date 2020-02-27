package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

func (s *Server) ReadAndPrepare(ctx context.Context,
	request *rpc.ReadAndPrepareRequest) (*rpc.ReadAndPrepareReply, error) {
	logrus.Infof("RECEIVE ReadAndPrepare %v", request.Txn.TxnId)
	requestOp := NewReadAndPrepareOp(request)

	s.scheduler.Schedule(requestOp)

	requestOp.BlockOwner()

	logrus.Infof("REPLY ReadAndPrepare %v", request.Txn.TxnId)

	return requestOp.GetReply(), nil
}

func (s *Server) Commit(ctx context.Context,
	request *rpc.CommitRequest) (*rpc.CommitReply, error) {
	logrus.Infof("RECEIVE Commit %v %v", request.TxnId, request.IsCoordinator)
	op := NewCommitRequestOp(request)
	if request.IsCoordinator {
		s.executor.CommitTxn <- op
	} else {
		s.coordinator.CommitRequest <- op
	}

	op.BlockOwner()
	logrus.Infof("REPLY Commit %v %v", request.TxnId, request.IsCoordinator)
	return &rpc.CommitReply{
		Result:     op.result,
		LeaderAddr: s.serverAddress,
	}, nil
}

func (s *Server) Abort(ctx context.Context,
	request *rpc.AbortRequest) (*rpc.AbortReply, error) {
	logrus.Infof("RECEIVE Abort %v %v", request.TxnId, request.IsCoordinator)
	op := NewAbortRequestOp(request, nil, false)
	if request.IsCoordinator {
		op.isFromCoordinator = true
		s.executor.AbortTxn <- op
	} else {
		s.coordinator.AbortRequest <- op
	}

	return &rpc.AbortReply{
		LeaderAddr: s.serverAddress,
	}, nil
}

func (s *Server) PrepareResult(ctx context.Context, request *rpc.PrepareResultRequest) (*rpc.PrepareResultReply, error) {
	logrus.Infof("RECEIVE PrepareResult %v partition %v result %v",
		request.TxnId, request.PartitionId, request.PrepareStatus)
	op := NewPrepareRequestOp(request, s.partitionId)
	s.coordinator.PrepareResult <- op
	return &rpc.PrepareResultReply{
		LeaderAddr: s.serverAddress,
	}, nil
}

func (s *Server) PrintStatus(cts context.Context, request *rpc.PrintStatusRequest) (*rpc.Empty, error) {
	op := NewPrintStatusRequestOp(int(request.CommittedTxn))
	s.executor.PrintStatus <- op
	op.BlockOwner()
	return &rpc.Empty{}, nil
}
