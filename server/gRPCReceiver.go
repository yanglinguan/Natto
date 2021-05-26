package server

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strconv"
	"time"
)

//type ReadAndPrepareResultSender struct {
//	stream rpc.Carousel_ReadAndPrepareServer
//	readResult chan *rpc.ReadAndPrepareReply
//}
//
//func NewReadAndPrepareResultSender(stream rpc.Carousel_ReadAndPrepareServer) *ReadAndPrepareResultSender {
//	s := &ReadAndPrepareResultSender{
//		stream: stream,
//		readResult: make(chan *rpc.ReadAndPrepareReply, 102400),
//	}
//	go s.run()
//	return s
//}
//
//func (r *ReadAndPrepareResultSender) run() {
//	for {
//		result := <- r.readResult
//		err := r.stream.Send(result)
//		if err != nil {
//			logrus.Fatalf("send error %v", err)
//		}
//	}
//}
//
//func (r *ReadAndPrepareResultSender) Send(result *rpc.ReadAndPrepareReply) {
//	r.readResult <- result
//}
//
//func (server *Server) ReadAndPrepare(stream rpc.Carousel_ReadAndPrepareServer) error {
//	logrus.Println("Started stream")
//	sender := NewReadAndPrepareResultSender(stream)
//	for {
//		request, err := stream.Recv()
//		logrus.Println("Received value")
//		if err == io.EOF {
//			return nil
//		}
//		if err != nil {
//			return err
//		}
//
//		if int(request.Txn.CoordPartitionId) == server.partitionId {
//			op := NewReadAndPrepareCoordinator(request)
//			server.coordinator.AddOperation(op)
//		}
//
//		if !request.IsNotParticipant {
//			requestOp := server.operationCreator.createReadAndPrepareOp(request, sender)
//			server.scheduler.AddOperation(requestOp)
//		}
//	}
//}

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

	server.scheduler.AddOperation(requestOp)
	//server.StartOp(requestOp)

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

	//if int(request.Txn.CoordPartitionId) == server.partitionId {
	//	op := NewReadAndPrepareCoordinator(request)
	//	server.coordinator.AddOperation(op)
	//}

	//if request.IsNotParticipant {
	//	reply := &rpc.ReadAndPrepareReply{
	//		KeyValVerList: make([]*rpc.KeyValueVersion, 0),
	//	}
	//
	//	return reply, nil
	//}

	requestOp := server.operationCreator.createReadOnlyOp(request)
	//server.StartOp(requestOp)
	server.scheduler.AddOperation(requestOp)
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
	server.scheduler.AddOperation(op)

	op.BlockClient()
	queuingDelay := time.Since(start)
	return &rpc.ProbeReply{
		QueuingDelay: queuingDelay.Nanoseconds(),
	}, nil
}

func (server *Server) ProbeTime(cts context.Context, request *rpc.ProbeReq) (*rpc.ProbeTimeReply, error) {
	op := NewProbeOp()
	server.scheduler.AddOperation(op)

	op.BlockClient()
	return &rpc.ProbeTimeReply{
		ProcessTime: time.Now().UnixNano(),
	}, nil
}

// client sends to coordinator
func (server *Server) ReadResultFromCoordinator(request *rpc.ReadRequestToCoordinator, srv rpc.Carousel_ReadResultFromCoordinatorServer) error {
	logrus.Debugf("server %v client send read result from coordinator from client %v", server.serverAddress, request.ClientId)
	//go server.coordinator.sendForwardResult(request.ClientId, srv)
	req := NewReadResultFromCoordinatorRequest(request.ClientId)
	server.readResultFromCoordinatorChan <- req
	req.block()

	for {
		result := <-req.replyChan
		logrus.Debugf("coordResult send result txn %v to client %v", result.TxnId, request.ClientId)
		err := srv.Send(result)
		if err != nil {
			log.Fatalf("stream cannot send to client %v txn %v error %v",
				request.ClientId, result.TxnId, err)
		}
		logrus.Debugf("coordResult result txn %v to client %v sent", result.TxnId, request.ClientId)
	}
	//op := NewReadRequestFromCoordinator(request, srv)
	//server.coordinator.AddOperation(op)
	return nil
}

// client sends write data to coordinator
func (server *Server) WriteData(cts context.Context, request *rpc.WriteDataRequest) (*rpc.Empty, error) {
	logrus.Debugf("server %v receive write data for txn %v", server.serverAddress, request.TxnId, request.TxnId)
	op := NewWriteDataOp(request)
	server.coordinator.AddOperation(op)
	return &rpc.Empty{}, nil
}

func (server *Server) ForwardReadRequestToCoordinator(cts context.Context, request *rpc.ForwardReadToCoordinator) (*rpc.Empty, error) {
	logrus.Debugf("server %v receive forward read request for txn %v", server.serverAddress, request.TxnId)
	op := NewForwardReadRequestToCoordinator(request)
	server.coordinator.AddOperation(op)
	return &rpc.Empty{}, nil
}

func (server *Server) CommitResultToCoordinator(cts context.Context, request *rpc.CommitResult) (*rpc.Empty, error) {
	logrus.Debugf("server %v receives commitResultToCoordinator request for txn %v commit result %v",
		server.serverAddress, request.TxnId, request.Result)
	op := NewCommitResultToCoordinator(request)
	server.coordinator.AddOperation(op)
	return &rpc.Empty{}, nil
}
