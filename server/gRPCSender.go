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
	connection *connection.Connection
}

func (p *PrepareResultSender) Send() {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	conn, err := p.connection.ConnectionPool.Get(context.Background())

	defer conn.Close()
	if err != nil {
		logrus.Fatalf("cannot get connection from the pool client send txn %v", p.connection.DstServerAddr)
	}

	client := rpc.NewCarouselClient(conn.ClientConn)
	_, err = client.PrepareResult(ctx, p.request)

	if err != nil {

	}

}

type AbortRequestSender struct {
	request    *rpc.AbortRequest
	timeout    time.Duration
	connection *connection.Connection
}

func (a *AbortRequestSender) Send() {
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()

	conn, err := a.connection.ConnectionPool.Get(context.Background())

	defer conn.Close()
	if err != nil {
		logrus.Fatalf("cannot get connection from the pool client send txn %v", a.connection.DstServerAddr)
	}

	client := rpc.NewCarouselClient(conn.ClientConn)
	_, err = client.Abort(ctx, a.request)
}

type CommitRequestSender struct {
	request    *rpc.CommitRequest
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
	_, err = client.Commit(ctx, c.request)
}
