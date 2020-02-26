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
	connection *connection.Connection
}

func (s *ReadAndPrepareSender) Send() {
	//ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	//defer cancel()

	conn, err := s.connection.ConnectionPool.Get(context.Background())

	defer conn.Close()
	if err != nil {
		logrus.Fatalf("cannot get connection from the pool client send txn %v", s.connection.DstServerAddr)
	}
	logrus.Infof("Send txn %v", s.request.Txn.TxnId)
	client := rpc.NewCarouselClient(conn.ClientConn)
	reply, err := client.ReadAndPrepare(context.Background(), s.request)
	if err == nil {
		logrus.Infof("Get Read result for txn %v", s.request.Txn.TxnId)
		s.txn.readAndPrepareReply <- reply
		logrus.Infof("log here")
	} else {
		logrus.Fatalf("rpc error %v", err)
	}
}

type CommitRequestSender struct {
	request    *rpc.CommitRequest
	txn        *Transaction
	timeout    time.Duration
	connection *connection.Connection
}

func (c *CommitRequestSender) Send() {
	//ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	//defer cancel()

	conn, err := c.connection.ConnectionPool.Get(context.Background())

	defer conn.Close()
	if err != nil {
		logrus.Fatalf("cannot get connection from the pool client send txn %v", c.connection.DstServerAddr)
	}
	logrus.Infof("send commit txn %v", c.request.TxnId)
	client := rpc.NewCarouselClient(conn.ClientConn)
	reply, err := client.Commit(context.Background(), c.request)
	if err == nil {
		logrus.Infof("get commit request %v", c.request.TxnId)
		c.txn.commitReply <- reply
	} else {
		logrus.Fatalf("rpc error %v", err)
	}
}
