package spanner

import (
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type connection struct {
	dstServerAddr string
	conn          *grpc.ClientConn
}

func newConnect(dstServerAddr string) *connection {
	c := &connection{
		dstServerAddr: dstServerAddr,
		conn:          nil,
	}

	conn, err := grpc.Dial(dstServerAddr, grpc.WithInsecure())

	if err != nil {
		logrus.Fatalf("cannot not connect: %v", err)
	}
	c.conn = conn
	return c
}

func (c *connection) GetConn() *grpc.ClientConn {
	return c.conn
}

func (c *connection) Close(conn *grpc.ClientConn) {
	err := conn.Close()
	if err != nil {
		logrus.Fatalf("cannot close connection to dst %v", c.dstServerAddr)
	}
}

func (c *connection) GetDstAddr() string {
	return c.dstServerAddr
}
