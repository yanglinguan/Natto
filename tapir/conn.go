package tapir

import (
	"sync"

	"google.golang.org/grpc"
)

// Server connection table
type ConnManager struct {
	connMap     map[string]*grpc.ClientConn // Network addr (ip:port) --> RPC connection
	connMapLock sync.RWMutex

	rpcStubMap     map[string]IrRpcClient // Network addr (ip:port)--> RPC stub
	rpcStubMapLock sync.RWMutex
}

func NewConnManager() *ConnManager {
	return &ConnManager{
		connMap:    make(map[string]*grpc.ClientConn),
		rpcStubMap: make(map[string]IrRpcClient),
	}
}

func (cm *ConnManager) BuildConnection(addr string) *grpc.ClientConn {
	cm.connMapLock.Lock()
	defer cm.connMapLock.Unlock()

	if conn, exists := cm.connMap[addr]; exists {
		return conn
	}

	logger.Debugf("Connecting to server %s", addr)
	var err error
	cm.connMap[addr], err = grpc.Dial(addr,
		grpc.WithInsecure(), grpc.WithReadBufferSize(0), grpc.WithWriteBufferSize(0))

	if err != nil {
		logger.Fatalf("Cannot build connection to server %s, error: %v", addr, err)
	}

	return cm.connMap[addr]
}

func (cm *ConnManager) GetConnection(addr string) (*grpc.ClientConn, bool) {
	cm.connMapLock.RLock()
	defer cm.connMapLock.RUnlock()

	conn, exists := cm.connMap[addr]
	return conn, exists
}

// Builds an RPC stub if not exists. Otherwise, returns the existing one
func (cm *ConnManager) BuildRpcStub(addr string) IrRpcClient {
	cm.rpcStubMapLock.Lock()
	defer cm.rpcStubMapLock.Unlock()

	if rpcStub, exists := cm.rpcStubMap[addr]; exists {
		return rpcStub
	}

	conn := cm.BuildConnection(addr)

	cm.rpcStubMap[addr] = NewIrRpcClient(conn)

	return cm.rpcStubMap[addr]
}

// Returns the RPC stub on file
func (cm *ConnManager) GetRpcStub(addr string) (IrRpcClient, bool) {
	cm.rpcStubMapLock.RLock()
	defer cm.rpcStubMapLock.RUnlock()

	rpcStub, exists := cm.rpcStubMap[addr]
	return rpcStub, exists
}

func (cm *ConnManager) NewRpcStub(addr string) IrRpcClient {
	conn, exists := cm.GetConnection(addr)
	if !exists {
		conn = cm.BuildConnection(addr)
	}

	rpcStub := NewIrRpcClient(conn)

	return rpcStub
}
