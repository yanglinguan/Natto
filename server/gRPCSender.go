package server

import (
	"Carousel-GTS/rpc"
	"Carousel-GTS/utils"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"time"
)

type PrepareResultSender struct {
	request     *rpc.PrepareResultRequest
	timeout     time.Duration
	dstServerId int
	server      *Server
}

func NewPrepareResultSender(request *rpc.PrepareResultRequest, dstServerId int, server *Server) *PrepareResultSender {
	s := &PrepareResultSender{
		request:     request,
		timeout:     0,
		server:      server,
		dstServerId: dstServerId,
	}
	return s
}

func (p *PrepareResultSender) Send() {
	logrus.Infof("SEND PrepareResult %v partition %v result %v to %v",
		p.request.TxnId, p.request.PartitionId, p.request.PrepareStatus, p.dstServerId)
	conn := p.server.connections[p.dstServerId]
	logrus.Infof("SEND PrepareResult %v partition %v result %v to %v",
		p.request.TxnId, p.request.PartitionId, p.request.PrepareStatus, conn.GetDstAddr())
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.PrepareResult(context.Background(), p.request)

	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			p.dstServerId = dstServerId
			p.Send()
		} else {
			logrus.Fatalf("fail to send prepare result txn %v to server %v: %v", p.request.TxnId, conn.GetDstAddr(), err)
		}
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
			logrus.Fatalf("cannot sent abort request txn %v to server %v: %v", a.request.TxnId, conn.GetDstAddr(), err)
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
			logrus.Fatalf("cannot send commit request txn %v to server %v: %v", c.request.TxnId, conn.GetDstAddr(), err)
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
