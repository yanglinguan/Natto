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
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	conn, err := s.connection.ConnectionPool.Get(context.Background())

	defer conn.Close()
	if err != nil {
		logrus.Fatalf("cannot get connection from the pool client send txn %v", s.connection.DstServerAddr)
	}

	client := rpc.NewCarouselClient(conn.ClientConn)
	reply, err := client.ReadAndPrepare(ctx, s.request)
	if err == nil {
		s.txn.readAndPrepareReply <- reply
	}
}

type CommitRequestSender struct {
	request    *rpc.CommitRequest
	txn        *Transaction
	timeout    time.Duration
	connection *connection.Connection
}

func (c *CommitRequestSender) Send() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	conn, err := c.connection.ConnectionPool.Get(context.Background())

	defer conn.Close()
	if err != nil {
		logrus.Fatalf("cannot get connection from the pool client send txn %v", c.connection.DstServerAddr)
	}

	client := rpc.NewCarouselClient(conn.ClientConn)
	reply, err := client.Commit(ctx, c.request)
	if err == nil {
		c.txn.commitReply <- reply
	}
}
