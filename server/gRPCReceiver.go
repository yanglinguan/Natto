package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
	"time"
)

func (server *Server) ReadAndPrepare(ctx context.Context,
	request *rpc.ReadAndPrepareRequest) (*rpc.ReadAndPrepareReply, error) {
	logrus.Infof("RECEIVE ReadAndPrepare %v readOnly %v",
		request.Txn.TxnId, request.Txn.ReadOnly)
	if !server.IsLeader() && (!server.config.GetFastPath() || request.IsNotParticipant) {
		logrus.Debugf("txn %v server %v is not leader", request.Txn.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}

	if int(request.Txn.CoordPartitionId) == server.partitionId {
		op := NewReadAndPrepareCoordinator(request)
		server.coordinator.AddOperation(op)
	}

	if request.IsNotParticipant {
		reply := &rpc.ReadAndPrepareReply{
			KeyValVerList: make([]*rpc.KeyValueVersion, 0),
		}

		return reply, nil
	}

	requestOp := server.operationCreator.createReadAndPrepareOp(request)

	server.StartOp(requestOp)

	// block until read result is ready
	requestOp.BlockClient()

	logrus.Debugf("REPLY ReadAndPrepare %v", request.Txn.TxnId)

	return requestOp.GetReadReply(), nil
}

func (server *Server) ReadOnly(cts context.Context, request *rpc.ReadAndPrepareRequest) (*rpc.ReadAndPrepareReply, error) {
	logrus.Infof("RECEIVE ReadOnlyRequest %v readOnly %v",
		request.Txn.TxnId, request.Txn.ReadOnly)
	if !server.IsLeader() && (!server.config.GetFastPath() || request.IsNotParticipant) {
		logrus.Debugf("txn %v server %v is not leader", request.Txn.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}

	if int(request.Txn.CoordPartitionId) == server.partitionId {
		op := NewReadAndPrepareCoordinator(request)
		server.coordinator.AddOperation(op)
	}

	if request.IsNotParticipant {
		reply := &rpc.ReadAndPrepareReply{
			KeyValVerList: make([]*rpc.KeyValueVersion, 0),
		}

		return reply, nil
	}

	requestOp := server.operationCreator.createReadOnlyOp(request)
	server.StartOp(requestOp)
	//server.scheduler.AddOperation(requestOp)
	requestOp.BlockClient()

	return requestOp.GetReadReply(), nil
}

func (server *Server) Commit(ctx context.Context,
	request *rpc.CommitRequest) (*rpc.CommitReply, error) {
	logrus.Infof("RECEIVE Commit %v %v", request.TxnId, request.FromCoordinator)
	if !server.IsLeader() {
		logrus.Debugf("txn %v server %v is not leader", request.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}

	if request.FromCoordinator {
		op := server.operationCreator.createCommitOp(request)
		server.storage.AddOperation(op)

		return &rpc.CommitReply{
			Result:   true,
			LeaderId: int32(server.GetLeaderServerId()),
		}, nil
	} else {
		op := NewCommitCoordinator(request)
		server.coordinator.AddOperation(op)
		op.blockClient()

		logrus.Infof("REPLY Commit %v %v", request.TxnId, request.FromCoordinator)
		return &rpc.CommitReply{
			Result:   op.result,
			LeaderId: int32(server.GetLeaderServerId()),
		}, nil
	}
}

func (server *Server) FastCommit(
	cts context.Context,
	request *rpc.FastCommitRequest) (*rpc.FastCommitReply, error) {
	op := NewFastCommitOp(request)
	server.commitScheduler.AddOperation(op)
	return &rpc.FastCommitReply{
		LeaderId: 0,
	}, nil
}

func (server *Server) FastAbort(
	cts context.Context,
	request *rpc.FastAbortRequest) (*rpc.FastAbortReply, error) {
	op := NewFastAbortOp(request)
	server.commitScheduler.AddOperation(op)
	return &rpc.FastAbortReply{
		LeaderId: 0,
	}, nil
}

func (server *Server) Abort(ctx context.Context,
	request *rpc.AbortRequest) (*rpc.AbortReply, error) {
	logrus.Infof("RECEIVE Abort %v %v", request.TxnId, request.FromCoordinator)
	if !server.IsLeader() {
		logrus.Debugf("txn %v server %v is not leader", request.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}

	if request.FromCoordinator {
		op := server.operationCreator.createAbortOp(request)
		server.storage.AddOperation(op)
	} else {
		op := NewAbortCoordinator(request)
		server.coordinator.AddOperation(op)
	}

	logrus.Infof("REPLY Abort %v %v", request.TxnId, request.FromCoordinator)
	return &rpc.AbortReply{
		LeaderId: int32(server.GetLeaderServerId()),
	}, nil
}

func (server *Server) PrepareResult(ctx context.Context, request *rpc.PrepareResultRequest) (*rpc.PrepareResultReply, error) {
	logrus.Infof("RECEIVE PrepareResult %v partition %v result %v",
		request.TxnId, request.PartitionId, TxnStatus(request.PrepareStatus).String())
	if !server.IsLeader() {
		logrus.Debugf("txn %v server %v is not leader", request.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}
	op := NewPrepareRequestOp(request)
	server.coordinator.AddOperation(op)

	return &rpc.PrepareResultReply{
		LeaderId: int32(server.GetLeaderServerId()),
	}, nil
}

func (server *Server) FastPrepareResult(ctx context.Context, request *rpc.FastPrepareResultRequest) (*rpc.FastPrepareResultReply, error) {
	logrus.Infof("RECEIVE FastPrepareResult %v partition %v result %v",
		request.PrepareResult.TxnId, request.PrepareResult.PartitionId, request.PrepareResult.PrepareStatus)
	if !server.IsLeader() {
		logrus.Debugf("txn %v server %v is not leader", request.PrepareResult.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}

	op := NewFastPrepareRequestOp(request)
	server.coordinator.AddOperation(op)
	return &rpc.FastPrepareResultReply{
		LeaderAddr: int32(server.GetLeaderServerId()),
	}, nil
}

func (server *Server) ReverseReorder(cts context.Context, request *rpc.ReverseReorderRequest) (*rpc.ReverseReorderReply, error) {
	logrus.Infof("RECEIVE reverse reorder txn %v requires to reverse reorder of txn %v on partition %v counter",
		request.TxnId, request.ReorderedTxnId, request.PartitionId, request.Counter)
	if !server.IsLeader() {
		logrus.Debugf("txn %v reverse reorder request server %v is not leader", request.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}

	op := NewReverseReorderRequest(request)
	server.coordinator.AddOperation(op)
	return &rpc.ReverseReorderReply{
		LeaderId: int32(server.GetLeaderServerId()),
	}, nil
}

func (server *Server) ReverseReorderAgreement(cts context.Context, request *rpc.ReverseAgreementRequest) (*rpc.ReverseAgreementReply, error) {
	logrus.Debugf("RECEIVE reverse reorder agreement txn %v (reordered txn %v) agreement %v partition %v counter %v",
		request.TxnId, request.ReorderedTxnId, request.PartitionId, request.Counter)

	if !server.IsLeader() {
		logrus.Debugf("txn %v reverse reorder request server %v is not leader", request.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}

	op := NewReverseReorderAgreement(request)
	server.coordinator.AddOperation(op)
	return &rpc.ReverseAgreementReply{
		LeaderId: int32(server.GetLeaderServerId()),
	}, nil
}

func (server *Server) RePrepare(cts context.Context, request *rpc.RePrepareRequest) (*rpc.RePrepareReply, error) {
	logrus.Debugf("Receive re-prepare txn %v re-prepare because of txn %v", request.TxnId, request.RequestTxnId)
	if !server.IsLeader() {
		logrus.Debugf("txn %v re-prepare request server %v is not leader", request.TxnId, server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}

	op := NewRePrepareRequest(request)
	server.storage.AddOperation(op)
	return &rpc.RePrepareReply{
		LeaderId: int32(server.getLeaderId()),
	}, nil
}

func (server *Server) PrintStatus(cts context.Context, request *rpc.PrintStatusRequest) (*rpc.Empty, error) {
	logrus.Infof("RECEIVE PrintStatus %v", request.CommittedTxn)
	op := NewPrintStatusRequestOp(int(request.CommittedTxn))
	server.storage.AddOperation(op)
	op.BlockClient()
	return &rpc.Empty{}, nil
}

func (server *Server) HeartBeat(cts context.Context, request *rpc.Empty) (*rpc.PrepareResultReply, error) {
	logrus.Info("RECEIVE HeartBeat")
	return &rpc.PrepareResultReply{
		LeaderId: int32(server.GetLeaderServerId()),
	}, nil
}

func (server *Server) Probe(cts context.Context, request *rpc.ProbeReq) (*rpc.ProbeReply, error) {
	op := NewProbeOp()
	start := time.Now()
	if request.FromCoordinator {
		server.commitScheduler.AddOperation(op)
	} else {
		server.scheduler.AddOperation(op)
	}
	op.BlockClient()
	queuingDelay := time.Since(start)
	return &rpc.ProbeReply{
		QueuingDelay: queuingDelay.Nanoseconds(),
	}, nil
}

func (server *Server) ProbeTime(cts context.Context, request *rpc.ProbeReq) (*rpc.ProbeTimeReply, error) {
	op := NewProbeOp()
	if request.FromCoordinator {
		server.commitScheduler.AddOperation(op)
	} else {
		server.scheduler.AddOperation(op)
	}
	op.BlockClient()
	return &rpc.ProbeTimeReply{
		ProcessTime: time.Now().UnixNano(),
	}, nil
}

func (server *Server) StartProbe(cts context.Context, request *rpc.StartProbeReq) (*rpc.Empty, error) {
	if !server.IsLeader() {
		logrus.Debugf("startProbe request server %v is not leader", server.serverAddress)
		return nil, status.Error(codes.Aborted, strconv.Itoa(server.GetLeaderServerId()))
	}

	server.coordinator.startProbe()
	return &rpc.Empty{}, nil
}
