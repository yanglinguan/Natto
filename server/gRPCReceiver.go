package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
)

func (server *Server) ReadAndPrepare(ctx context.Context,
	request *rpc.ReadAndPrepareRequest) (*rpc.ReadAndPrepareReply, error) {
	logrus.Infof("RECEIVE ReadAndPrepare %v", request.Txn.TxnId)
	if !server.IsLeader() {
		logrus.Debugf("txn %v server %v is not leader", request.Txn.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}
	requestOp := NewReadAndPrepareOp(request, server)

	if int(request.Txn.CoordPartitionId) == server.partitionId {
		server.coordinator.Wait2PCResultTxn <- requestOp
	}

	if request.IsNotParticipant {
		if int(request.Txn.CoordPartitionId) != server.partitionId {
			logrus.Fatalf("txn %v is not participant and not coordinator (server %v, pId %v)",
				request.Txn.TxnId,
				server.serverId,
				server.partitionId)
		}

		reply := &rpc.ReadAndPrepareReply{
			KeyValVerList: make([]*rpc.KeyValueVersion, 0),
		}

		return reply, nil
	}

	server.scheduler.Schedule(requestOp)

	requestOp.BlockOwner()

	logrus.Infof("REPLY ReadAndPrepare %v", request.Txn.TxnId)

	return requestOp.GetReply(), nil
}

func (server *Server) Commit(ctx context.Context,
	request *rpc.CommitRequest) (*rpc.CommitReply, error) {
	logrus.Infof("RECEIVE Commit %v %v", request.TxnId, request.IsCoordinator)
	if !server.IsLeader() {
		logrus.Debugf("txn %v server %v is not leader", request.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}
	op := NewCommitRequestOp(request)
	if request.IsCoordinator {
		server.executor.CommitTxn <- op
	} else {
		server.coordinator.CommitRequest <- op
	}

	op.BlockOwner()
	logrus.Infof("REPLY Commit %v %v", request.TxnId, request.IsCoordinator)
	return &rpc.CommitReply{
		Result:   op.result,
		LeaderId: int32(server.GetLeaderServerId()),
	}, nil
}

func (server *Server) Abort(ctx context.Context,
	request *rpc.AbortRequest) (*rpc.AbortReply, error) {
	logrus.Infof("RECEIVE Abort %v %v", request.TxnId, request.IsCoordinator)
	if !server.IsLeader() {
		logrus.Debugf("txn %v server %v is not leader", request.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}
	op := NewAbortRequestOp(request, nil, false)
	if request.IsCoordinator {
		op.isFromCoordinator = true
		server.executor.AbortTxn <- op
	} else {
		server.coordinator.AbortRequest <- op
	}
	logrus.Infof("REPLY Abort %v %v", request.TxnId, request.IsCoordinator)
	return &rpc.AbortReply{
		LeaderId: int32(server.GetLeaderServerId()),
	}, nil
}

func (server *Server) PrepareResult(ctx context.Context, request *rpc.PrepareResultRequest) (*rpc.PrepareResultReply, error) {
	logrus.Infof("RECEIVE PrepareResult %v partition %v result %v",
		request.TxnId, request.PartitionId, request.PrepareStatus)
	if !server.IsLeader() {
		logrus.Debugf("txn %v server %v is not leader", request.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}
	op := NewPrepareRequestOp(request, server.partitionId)
	server.coordinator.PrepareResult <- op
	return &rpc.PrepareResultReply{
		LeaderId: int32(server.GetLeaderServerId()),
	}, nil
}

func (server *Server) PrintStatus(cts context.Context, request *rpc.PrintStatusRequest) (*rpc.Empty, error) {
	logrus.Infof("RECEIVE PrintStatus %v", request.CommittedTxn)
	op := NewPrintStatusRequestOp(int(request.CommittedTxn))
	server.executor.PrintStatus <- op
	op.BlockOwner()
	return &rpc.Empty{}, nil
}

func (server *Server) HeartBeat(cts context.Context, request *rpc.Empty) (*rpc.PrepareResultReply, error) {
	logrus.Info("RECEIVE HeartBeat")
	return &rpc.PrepareResultReply{
		LeaderId: int32(server.GetLeaderServerId()),
	}, nil
}
