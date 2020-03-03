package server

import (
	"Carousel-GTS/connection"
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"time"
)

type PrepareResultSender struct {
	request    *rpc.PrepareResultRequest
	timeout    time.Duration
	connection connection.Connection
}

func NewPrepareResultSender(request *rpc.PrepareResultRequest, connection connection.Connection) *PrepareResultSender {
	s := &PrepareResultSender{
		request:    request,
		timeout:    0,
		connection: connection,
	}
	return s
}

func (p *PrepareResultSender) Send() {
	logrus.Infof("SEND PrepareResult %v partition %v result %v to %v",
		p.request.TxnId, p.request.PartitionId, p.request.PrepareStatus, p.connection.GetDstAddr())
	conn := p.connection.GetConn()
	if p.connection.GetPoolSize() > 0 {
		defer p.connection.Close(conn)
	}

	client := rpc.NewCarouselClient(conn)
	_, err := client.PrepareResult(context.Background(), p.request)

	if err != nil {
		logrus.Fatalf("fail to send prepare result: %v", err)
	}
}

type AbortRequestSender struct {
	request    *rpc.AbortRequest
	timeout    time.Duration
	connection connection.Connection
}

func NewAbortRequestSender(request *rpc.AbortRequest, connection connection.Connection) *AbortRequestSender {
	s := &AbortRequestSender{
		request:    request,
		timeout:    0,
		connection: connection,
	}
	return s
}

func (a *AbortRequestSender) Send() {
	logrus.Infof("SEND AbortRequest %v to %v", a.request.TxnId, a.connection.GetDstAddr())
	conn := a.connection.GetConn()
	if a.connection.GetPoolSize() > 0 {
		defer a.connection.Close(conn)
	}

	client := rpc.NewCarouselClient(conn)
	_, err := client.Abort(context.Background(), a.request)
	if err != nil {
		logrus.Fatalf("cannot sent abort request: %v", err)
	}
}

type CommitRequestSender struct {
	request    *rpc.CommitRequest
	timeout    time.Duration
	connection connection.Connection
}

func NewCommitRequestSender(request *rpc.CommitRequest, connection connection.Connection) *CommitRequestSender {
	s := &CommitRequestSender{
		request:    request,
		timeout:    0,
		connection: connection,
	}
	return s
}

func (c *CommitRequestSender) Send() {
	logrus.Infof("SEND Commit %v (coordinator) to %v", c.request.TxnId, c.connection.GetDstAddr())
	conn := c.connection.GetConn()
	if c.connection.GetPoolSize() > 0 {
		defer c.connection.Close(conn)
	}

	client := rpc.NewCarouselClient(conn)

	_, err := client.Commit(context.Background(), c.request)

	if err != nil {
		logrus.Fatalf("cannot send commit request: %v", err)
	}

}
