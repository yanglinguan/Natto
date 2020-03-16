package client

import (
	"Carousel-GTS/connection"
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"time"
)

type ReadAndPrepareSender struct {
	request    *rpc.ReadAndPrepareRequest
	txn        *Transaction
	timeout    time.Duration
	connection connection.Connection
}

func NewReadAndPrepareSender(request *rpc.ReadAndPrepareRequest,
	txn *Transaction, connection connection.Connection) *ReadAndPrepareSender {
	r := &ReadAndPrepareSender{
		request:    request,
		txn:        txn,
		timeout:    0,
		connection: connection,
	}

	return r
}

func (s *ReadAndPrepareSender) Send() {
	conn := s.connection.GetConn()
	if s.connection.GetPoolSize() > 0 {
		defer s.connection.Close(conn)
	}

	client := rpc.NewCarouselClient(conn)
	logrus.Infof("SEND ReadAndPrepare %v to %v", s.request.Txn.TxnId, s.connection.GetDstAddr())

	reply, err := client.ReadAndPrepare(context.Background(), s.request)
	if err == nil {
		logrus.Infof("RECEIVE ReadResult %v from %v", s.request.Txn.TxnId, s.connection.GetDstAddr())
		s.txn.readAndPrepareReply <- reply
	} else {
		logrus.Fatalf("cannot send txn %v readAndPrepare to server %v: %v", s.request.Txn.TxnId, s.connection.GetDstAddr(), err)
	}
}

type CommitRequestSender struct {
	request    *rpc.CommitRequest
	txn        *Transaction
	timeout    time.Duration
	connection connection.Connection
}

func NewCommitRequestSender(request *rpc.CommitRequest,
	txn *Transaction, connection connection.Connection) *CommitRequestSender {
	s := &CommitRequestSender{
		request:    request,
		txn:        txn,
		timeout:    0,
		connection: connection,
	}
	return s
}

func (c *CommitRequestSender) Send() {
	conn := c.connection.GetConn()
	if c.connection.GetPoolSize() > 0 {
		defer c.connection.Close(conn)
	}

	client := rpc.NewCarouselClient(conn)
	logrus.Infof("SEND Commit %v to %v", c.request.TxnId, c.connection.GetDstAddr())
	reply, err := client.Commit(context.Background(), c.request)
	if err == nil {
		logrus.Infof("RECEIVE CommitResult %v from %v", c.request.TxnId, c.connection.GetDstAddr())
		c.txn.commitReply <- reply
	} else {
		logrus.Fatalf("cannot send txn %v commit request to server %v: %v", c.request.TxnId, c.connection.GetDstAddr(), err)
	}
}

type PrintStatusRequestSender struct {
	request    *rpc.PrintStatusRequest
	connection connection.Connection
}

func NewPrintStatusRequestSender(request *rpc.PrintStatusRequest, connection connection.Connection) *PrintStatusRequestSender {
	p := &PrintStatusRequestSender{
		request:    request,
		connection: connection,
	}
	return p
}

func (p *PrintStatusRequestSender) Send() {
	conn := p.connection.GetConn()
	if p.connection.GetPoolSize() > 0 {
		defer p.connection.Close(conn)
	}

	client := rpc.NewCarouselClient(conn)
	logrus.Infof("SEND PrintStatus to %v ", p.connection.GetDstAddr())
	_, err := client.PrintStatus(context.Background(), p.request)
	if err != nil {
		logrus.Fatalf("cannot sent print status request to server %v: %v", p.connection.GetDstAddr(), err)
	}
}
