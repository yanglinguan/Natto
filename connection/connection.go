package connection

import (
	"context"
	"github.com/processout/grpc-go-pool"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Connection interface {
	GetConn() *grpc.ClientConn
	GetDstAddr() string
	Close(conn *grpc.ClientConn)
	GetPoolSize() int
}

type PoolConnection struct {
	dstServerAddr  string
	connectionPool *grpcpool.Pool
	connMap        map[*grpc.ClientConn]*grpcpool.ClientConn
	size           int
}

func (c *PoolConnection) createPool(size int) {
	var factory grpcpool.Factory
	factory = func() (*grpc.ClientConn, error) {
		conn, err := grpc.Dial(c.dstServerAddr, grpc.WithInsecure())
		if err != nil {
			logrus.Fatalf("Failed to start gRPC connection: %v", err)
		}
		logrus.Infof("Connected to server at %s", c.dstServerAddr)
		return conn, err
	}
	pool, err := grpcpool.New(factory, 0, size, 0)
	if err != nil {
		logrus.Fatalf("Failed to create gRPC pool: %v", err)
	}
	c.connectionPool = pool
}

func NewPoolConnection(dstServerAddr string, poolSize int) *PoolConnection {
	c := &PoolConnection{
		dstServerAddr:  dstServerAddr,
		connectionPool: nil,
		size:           poolSize,
		connMap:        make(map[*grpc.ClientConn]*grpcpool.ClientConn),
	}
	c.createPool(poolSize)
	return c
}

func (c *PoolConnection) GetConn() *grpc.ClientConn {
	conn, err := c.connectionPool.Get(context.Background())
	if err != nil || conn == nil {
		logrus.Fatalf("cannot get connection from the pool client send txn %v", c.dstServerAddr)
		return nil
	}
	c.connMap[conn.ClientConn] = conn
	return conn.ClientConn
}

func (c *PoolConnection) Close(conn *grpc.ClientConn) {
	err := c.connMap[conn].Close()
	if err != nil {
		logrus.Fatalf("cannot close connection to %v", c.dstServerAddr)
	}
}

func (c *PoolConnection) GetDstAddr() string {
	return c.dstServerAddr
}

func (c *PoolConnection) GetPoolSize() int {
	return c.size
}

type SingleConnection struct {
	dstServerAddr string
	conn          *grpc.ClientConn
}

func NewSingleConnect(dstServerAddr string) *SingleConnection {
	c := &SingleConnection{
		dstServerAddr: dstServerAddr,
		conn:          nil,
	}

	conn, err := grpc.Dial(dstServerAddr, grpc.WithInsecure(),
		grpc.WithReadBufferSize(0), grpc.WithWriteBufferSize(0))

	if err != nil {
		logrus.Fatalf("cannot not connect: %v", err)
	}
	c.conn = conn
	return c
}

func (c *SingleConnection) GetConn() *grpc.ClientConn {
	return c.conn
}

func (c *SingleConnection) Close(conn *grpc.ClientConn) {
	err := conn.Close()
	if err != nil {
		logrus.Fatalf("cannot close connection to dst %v", c.dstServerAddr)
	}
}

func (c *SingleConnection) GetDstAddr() string {
	return c.dstServerAddr
}

func (c *SingleConnection) GetPoolSize() int {
	return 0
}
