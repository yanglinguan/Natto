package server

import (
	"Carousel-GTS/rpc"
	"golang.org/x/net/context"
)

func (s *Server) ReadAndPrepare(ctx context.Context,
	request *rpc.ReadAndPrepareRequest) (*rpc.ReadAndPrepareReply, error) {
	requestOp := &ReadAndPrepareOp{
		request:             request,
		wait:                make(chan bool, 1),
		readKeyMap:          make(map[string]bool),
		writeKeyMap:         make(map[string]bool),
		preparedWriteKeyNum: 0,
		preparedReadKeyNum:  0,
	}
	for _, rk := range request.Txn.ReadKeyList {
		requestOp.readKeyMap[rk] = false
	}
	for _, wk := range request.Txn.WriteKeyList {
		requestOp.writeKeyMap[wk] = false
	}

	requestOp.numPartitions = len(request.Txn.ParticipatedPartitionIds)

	s.scheduler.Schedule(requestOp)

	requestOp.BlockOwner()

	return requestOp.GetReply(), nil
}

func (s *Server) Commit(ctx context.Context,
	request *rpc.CommitRequest) (*rpc.CommitReply, error) {
	op := &CommitRequestOp{
		request:   request,
		canCommit: false,
		wait:      make(chan bool, 1),
	}
	if request.IsCoordinator {
		s.executor.CommitTxn <- op
	} else {
		s.coordinator.CommitRequest <- op
	}

	op.BlockOwner()

	return &rpc.CommitReply{
		Result:     op.result,
		LeaderAddr: s.serverAddress,
	}, nil
}

func (s *Server) Abort(ctx context.Context,
	request *rpc.AbortRequest) (*rpc.AbortReply, error) {
	op := &AbortRequestOp{
		abortRequest:      request,
		request:           nil,
		isFromCoordinator: false,
		sendToCoordinator: false,
	}
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
	op := &PrepareResultOp{
		Request:          request,
		CoordPartitionId: s.partitionId,
	}
	s.coordinator.PrepareResult <- op
	return &rpc.PrepareResultReply{
		LeaderAddr: s.serverAddress,
	}, nil
}
