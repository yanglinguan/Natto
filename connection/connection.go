package connection

import (
	"github.com/processout/grpc-go-pool"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Connection struct {
	DstServerAddr  string
	ConnectionPool *grpcpool.Pool
}

func (c *Connection) createPool(size int) {
	var factory grpcpool.Factory
	factory = func() (*grpc.ClientConn, error) {
		conn, err := grpc.Dial(c.DstServerAddr, grpc.WithInsecure())
		if err != nil {
			logrus.Fatalf("Failed to start gRPC connection: %v", err)
		}
		logrus.Infof("Connected to server at %s", c.DstServerAddr)
		return conn, err
	}
	pool, err := grpcpool.New(factory, 0, size, 0)
	if err != nil {
		logrus.Fatalf("Failed to create gRPC pool: %v", err)
	}
	c.ConnectionPool = pool
}

func NewConnection(dstServerAddr string, poolSize int) *Connection {
	c := &Connection{
		DstServerAddr:  dstServerAddr,
		ConnectionPool: nil,
	}
	c.createPool(poolSize)
	return c
}
