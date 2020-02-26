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
	logrus.Infof("SEND PrepareResult %v partition %v result %v",
		p.request.TxnId, p.request.PartitionId, p.request.PrepareStatus)
	conn := p.connection.GetConn()
	if p.connection.GetPoolSize() > 0 {
		defer p.connection.Close(conn)
	}

	client := rpc.NewCarouselClient(conn)
	_, err := client.PrepareResult(context.Background(), p.request)

	if err != nil {
		logrus.Errorf("fail to send prepare result")
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
	logrus.Infof("SEND AbortRequest %v", a.request.TxnId)
	conn := a.connection.GetConn()
	if a.connection.GetPoolSize() > 0 {
		defer a.connection.Close(conn)
	}

	client := rpc.NewCarouselClient(conn)
	_, err := client.Abort(context.Background(), a.request)
	if err != nil {
		logrus.Errorf("cannot sent abort request")
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
	logrus.Infof("SEND Commit %v (coordinator)", c.request.TxnId)
	conn := c.connection.GetConn()
	if c.connection.GetPoolSize() > 0 {
		defer c.connection.Close(conn)
	}

	client := rpc.NewCarouselClient(conn)

	_, err := client.Commit(context.Background(), c.request)

	if err != nil {
		logrus.Errorf("cannot send commit request")
	}

}
