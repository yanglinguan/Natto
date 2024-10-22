package server

import (
	"Carousel-GTS/rpc"
	"Carousel-GTS/utils"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"time"
)

type PrepareResultSender struct {
	prepareResult *rpc.PrepareResultRequest
	timeout       time.Duration
	dstServerId   int
	server        *Server
	//coordinatorPartition int
}

func NewPrepareResultSender(request *rpc.PrepareResultRequest, dstServerId int, server *Server) *PrepareResultSender {
	s := &PrepareResultSender{
		prepareResult: request,
		timeout:       0,
		server:        server,
		dstServerId:   dstServerId,
	}
	return s
}

func (p *PrepareResultSender) Send() {
	if p.dstServerId == p.server.serverId {
		op := NewPrepareRequestOp(p.prepareResult)
		p.server.coordinator.AddOperation(op)
		return
	}
	logrus.Infof("SEND PrepareResult %v partition %v result %v to %v",
		p.prepareResult.TxnId, p.prepareResult.PartitionId, p.prepareResult.PrepareStatus, p.dstServerId)
	conn := p.server.connections[p.dstServerId]
	logrus.Infof("SEND PrepareResult %v partition %v result %v to %v",
		p.prepareResult.TxnId, p.prepareResult.PartitionId, p.prepareResult.PrepareStatus, conn.GetDstAddr())
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.PrepareResult(context.Background(), p.prepareResult)

	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			p.dstServerId = dstServerId
			p.Send()
		} else {
			logrus.Fatalf("fail to send prepare result txn %v to server %v: %v", p.prepareResult.TxnId, conn.GetDstAddr(), err)
		}
	}
}

type ReverseReorderRequestSender struct {
	request     *rpc.ReverseReorderRequest
	dstServerId int
	server      *Server
	timeout     time.Duration
}

func NewReverseReorderRequestSender(request *rpc.ReverseReorderRequest, dstServerId int, server *Server) *ReverseReorderRequestSender {
	r := &ReverseReorderRequestSender{
		request:     request,
		dstServerId: dstServerId,
		server:      server,
		timeout:     0,
	}

	return r
}

func (r *ReverseReorderRequestSender) Send() {
	if r.dstServerId == r.server.serverId {
		op := NewReverseReorderRequest(r.request)
		r.server.coordinator.AddOperation(op)
		return
	}
	logrus.Infof("txn %v SEND ReverseReorder to reverse txn %v on partition %v",
		r.request.TxnId, r.request.ReorderedTxnId, r.request.PartitionId)
	conn := r.server.connections[r.dstServerId]
	//logrus.Infof("SEND PrepareResult %v partition %v result %v to %v",
	//	p.prepareResult.TxnId, p.prepareResult.PartitionId, p.prepareResult.PrepareStatus, conn.GetDstAddr())
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.ReverseReorder(context.Background(), r.request)

	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			r.dstServerId = dstServerId
			r.Send()
		} else {
			logrus.Fatalf("txn %v fail to send reverse reorder to server %v: %v", r.request.TxnId, conn.GetDstAddr(), err)
		}
	}

}

type ReverseReorderAgreementSender struct {
	request     *rpc.ReverseAgreementRequest
	dstServerId int
	server      *Server
	timeout     time.Duration
}

func NewReverseReorderAgreementSender(request *rpc.ReverseAgreementRequest, dstServerId int, server *Server) *ReverseReorderAgreementSender {
	s := &ReverseReorderAgreementSender{
		request:     request,
		dstServerId: dstServerId,
		server:      server,
		timeout:     0,
	}

	return s
}

func (s *ReverseReorderAgreementSender) Send() {
	if s.dstServerId == s.server.serverId {
		op := NewReverseReorderAgreement(s.request)
		s.server.coordinator.AddOperation(op)
		return
	}

	conn := s.server.connections[s.dstServerId]
	logrus.Infof("SEND reverse reorder agreement (txn %v request reverser reorder of txn %v) to %v",
		s.request.TxnId,
		s.request.ReorderedTxnId,
		conn.GetDstAddr())
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.ReverseReorderAgreement(context.Background(), s.request)
	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			s.dstServerId = dstServerId
			s.Send()
		} else {
			logrus.Fatalf("cannot sent reverse reorder agreement (txn %v request reverser reorder of txn %v) to %v: %v",
				s.request.TxnId,
				s.request.ReorderedTxnId,
				conn.GetDstAddr(), err)
		}
	} else {
		logrus.Infof("RECEIVE ACK Reverse reorder agreement (txn %v request reverser reorder of txn %v) from server %v",
			s.request.TxnId,
			s.request.ReorderedTxnId,
			conn.GetDstAddr())
	}
}

type RePrepareSender struct {
	request     *rpc.RePrepareRequest
	dstServerId int
	server      *Server
	timeout     time.Duration
}

func NewRePrepareSender(request *rpc.RePrepareRequest, dstServerId int, server *Server) *RePrepareSender {
	s := &RePrepareSender{
		request:     request,
		dstServerId: dstServerId,
		server:      server,
		timeout:     0,
	}

	return s
}

func (s *RePrepareSender) Send() {
	if s.dstServerId == s.server.serverId {
		op := NewRePrepareRequest(s.request)
		s.server.storage.AddOperation(op)
		return
	}

	conn := s.server.connections[s.dstServerId]
	logrus.Infof("SEND re-prepare (txn %v re-prepare because of txn %v) to %v",
		s.request.TxnId,
		s.request.RequestTxnId,
		conn.GetDstAddr())
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.RePrepare(context.Background(), s.request)
	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			s.dstServerId = dstServerId
			s.Send()
		} else {
			logrus.Fatalf("cannot sent re-prepare (txn %v re-prepare because of txn %v) to %v: %v",
				s.request.TxnId,
				s.request.RequestTxnId,
				conn.GetDstAddr(), err)
		}
	} else {
		logrus.Infof("RECEIVE ACK re-prepare (txn %v re-prepare because of txn %v) from server %v",
			s.request.TxnId,
			s.request.RequestTxnId,
			conn.GetDstAddr())
	}
}

type AbortRequestSender struct {
	request     *rpc.AbortRequest
	timeout     time.Duration
	dstServerId int
	server      *Server
}

func NewAbortRequestSender(request *rpc.AbortRequest, dstServerId int, server *Server) *AbortRequestSender {
	s := &AbortRequestSender{
		request:     request,
		timeout:     0,
		dstServerId: dstServerId,
		server:      server,
	}
	return s
}

func (a *AbortRequestSender) Send() {
	if a.server.serverId == a.dstServerId {
		op := a.server.operationCreator.createAbortOp(a.request)
		a.server.storage.AddOperation(op)
		return
	}
	conn := a.server.connections[a.dstServerId]
	logrus.Infof("SEND AbortRequest %v to %v", a.request.TxnId, conn.GetDstAddr())
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.Abort(context.Background(), a.request)
	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			a.dstServerId = dstServerId
			a.Send()
		} else {
			logrus.Fatalf("cannot sent abort prepareResult txn %v to server %v: %v", a.request.TxnId, conn.GetDstAddr(), err)
		}
	} else {
		logrus.Infof("RECEIVE ACK Abort %v from server %v", a.request.TxnId, conn.GetDstAddr())
	}
}

type CommitRequestSender struct {
	request     *rpc.CommitRequest
	timeout     time.Duration
	dstServerId int
	server      *Server
}

func NewCommitRequestSender(request *rpc.CommitRequest, dstServerId int, server *Server) *CommitRequestSender {
	s := &CommitRequestSender{
		request:     request,
		timeout:     0,
		dstServerId: dstServerId,
		server:      server,
	}
	return s
}

func (c *CommitRequestSender) Send() {
	if c.server.serverId == c.dstServerId {
		op := c.server.operationCreator.createCommitOp(c.request)
		c.server.storage.AddOperation(op)
		return
	}
	conn := c.server.connections[c.dstServerId]
	logrus.Infof("SEND Commit %v (coordinator) to %v", c.request.TxnId, conn.GetDstAddr())
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)

	_, err := client.Commit(context.Background(), c.request)

	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			c.dstServerId = dstServerId
			c.Send()
		} else {
			logrus.Fatalf("cannot send commit prepareResult txn %v to server %v: %v", c.request.TxnId, conn.GetDstAddr(), err)
		}
	} else {
		logrus.Infof("RECEIVE ACK Commit %v from server %v", c.request.TxnId, conn.GetDstAddr())
	}
}

type FastPrepareResultSender struct {
	request     *rpc.FastPrepareResultRequest
	timeout     time.Duration
	dstServerId int
	server      *Server
}

func NewFastPrepareResultSender(request *rpc.FastPrepareResultRequest, dstServerId int, server *Server) *FastPrepareResultSender {
	s := &FastPrepareResultSender{
		request:     request,
		timeout:     0,
		server:      server,
		dstServerId: dstServerId,
	}
	return s
}

func (p *FastPrepareResultSender) Send() {
	if p.server.serverId == p.dstServerId {
		op := NewFastPrepareRequestOp(p.request)
		p.server.coordinator.AddOperation(op)
		return
	}
	conn := p.server.connections[p.dstServerId]
	logrus.Infof("SEND PrepareResult %v partition %v result %v to %v",
		p.request.PrepareResult.TxnId, p.request.PrepareResult.PartitionId, p.request.PrepareResult.PrepareStatus, conn.GetDstAddr())
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.FastPrepareResult(context.Background(), p.request)

	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			p.dstServerId = dstServerId
			p.Send()
		} else {
			logrus.Fatalf("fail to send prepare result txn %v to server %v: %v", p.request.PrepareResult.TxnId, conn.GetDstAddr(), err)
		}
	}
}

type ProbeSender struct {
	dstServerId int
	server      *Server
}

func NewProbeSender(dstServerId int, server *Server) *ProbeSender {
	s := &ProbeSender{
		dstServerId: dstServerId,
		server:      server,
	}
	return s
}

func (p *ProbeSender) Send() int64 {
	conn := p.server.connections[p.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	reply, err := client.Probe(context.Background(), &rpc.ProbeReq{
		FromCoordinator: true,
	})
	if err != nil {
		logrus.Fatalf("cannot sent probe request to server %v: %v", conn.GetDstAddr(), err)
		return -1
	}
	return reply.QueuingDelay
}

type ProbeTimeSender struct {
	dstServerId int
	server      *Server
}

func NewProbeTimeSender(dstServerId int, server *Server) *ProbeTimeSender {
	s := &ProbeTimeSender{
		dstServerId: dstServerId,
		server:      server,
	}
	return s
}

func (p *ProbeTimeSender) Send() int64 {
	logrus.Debugf("send to server %v conn %v", p.dstServerId, p.server.connections[p.dstServerId])
	conn := p.server.connections[p.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	reply, err := client.ProbeTime(context.Background(), &rpc.ProbeReq{
		FromCoordinator: true,
	})
	if err != nil {
		logrus.Fatalf("cannot sent probeTime request to server %v: %v", conn.GetDstAddr(), err)
		return -1
	}
	return reply.ProcessTime
}

type ForwardReadRequestToCoordinatorSender struct {
	request     *rpc.ForwardReadToCoordinator
	timeout     time.Duration
	dstServerId int
	server      *Server
}

func NewForwardReadRequestToCoordinatorSender(request *rpc.ForwardReadToCoordinator, dstServerId int, server *Server) *ForwardReadRequestToCoordinatorSender {
	s := &ForwardReadRequestToCoordinatorSender{
		request:     request,
		timeout:     0,
		dstServerId: dstServerId,
		server:      server,
	}

	return s
}

func (p *ForwardReadRequestToCoordinatorSender) Send() {
	if p.dstServerId == p.server.serverId {
		op := NewForwardReadRequestToCoordinator(p.request)
		p.server.coordinator.AddOperation(op)
		return
	}

	conn := p.server.connections[p.dstServerId]
	logrus.Debugf("SEND ForwardReadRequestToCoordinator txn %v from server %v to %v",
		p.request.TxnId, p.server.serverAddress, conn.GetDstAddr())
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.ForwardReadRequestToCoordinator(context.Background(), p.request)

	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			p.dstServerId = dstServerId
			p.Send()
		} else {
			logrus.Fatalf("fail to send prepare result txn %v to server %v: %v", p.request.TxnId, conn.GetDstAddr(), err)
		}
	}
}

type CommitResultToCoordinatorSender struct {
	request     *rpc.CommitResult
	timeout     time.Duration
	dstServerId int
	server      *Server
}

func NewCommitResultToCoordinatorSender(request *rpc.CommitResult, dstServerId int, server *Server) *CommitResultToCoordinatorSender {
	s := &CommitResultToCoordinatorSender{
		request:     request,
		timeout:     0,
		dstServerId: dstServerId,
		server:      server,
	}

	return s
}

func (p *CommitResultToCoordinatorSender) Send() {
	if p.dstServerId == p.server.serverId {
		op := NewCommitResultToCoordinator(p.request)
		p.server.coordinator.AddOperation(op)
		return
	}

	conn := p.server.connections[p.dstServerId]
	logrus.Debugf("SEND CommitResultToCoordinatorSender txn %v from server %v to %v",
		p.request.TxnId, p.server.serverAddress, conn.GetDstAddr())
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.CommitResultToCoordinator(context.Background(), p.request)

	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			p.dstServerId = dstServerId
			p.Send()
		} else {
			logrus.Fatalf("fail to send prepare result txn %v to server %v: %v", p.request.TxnId, conn.GetDstAddr(), err)
		}
	}
}
