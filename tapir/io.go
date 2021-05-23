package tapir

import (
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type IO interface {
	InitConn(addrList []string)
	Shutdown()

	ProposeUnLogOp(addr string, op []byte) *UnLogReplyMsg
	ProposeIncOp(addr string, reqId string, op []byte)
	FinalizeIncOp(addr string, reqId string)
	ProposeConOp(addr string, reqId string, op []byte) *ConReplyMsg
	FinalizeConOp(addr string, reqId string, ret string)

	// Probing
	Probe(addr string) *ProbeReply

	// Testing
	Test(addr string)
}

type grpcIo struct {
	cm *ConnManager
}

func NewGrpcIo() IO {
	gIo := &grpcIo{
		cm: NewConnManager(),
	}
	return gIo
}

func (gIo *grpcIo) InitConn(addrList []string) {
	var wg sync.WaitGroup
	for _, addr := range addrList {
		wg.Add(1)
		go func(addr string) {
			gIo.cm.BuildConnection(addr)
			wg.Done()
		}(addr)
	}
	wg.Wait()
}

func (gIo *grpcIo) Shutdown() {
}

func (gIo *grpcIo) ProposeUnLogOp(addr string, op []byte) *UnLogReplyMsg {
	req := &UnLogReqMsg{
		Op: op,
	}
	rpcStub := gIo.cm.NewRpcStub(addr)
	reply, err := rpcStub.ProposeUnLogOp(context.Background(), req, grpc.WaitForReady(true))

	if err != nil {
		logger.Errorf("Error: %v", err)
		logger.Fatalf("Fails sending ProposeUnLogOp request to addr = %s", addr)
	}
	return reply
}

func (gIo *grpcIo) ProposeIncOp(addr string, reqId string, op []byte) {
	req := &IncReqMsg{
		Req: &Request{
			Id: reqId,
			Op: op,
		},
	}
	rpcStub := gIo.cm.NewRpcStub(addr)
	_, err := rpcStub.ProposeIncOp(context.Background(), req, grpc.WaitForReady(true))

	if err != nil {
		logger.Errorf("Error: %v", err)
		logger.Fatalf("Fails sending ProposeIncOp request to addr = %s IR reqId = %s", addr, reqId)
	}
}

func (gIo *grpcIo) FinalizeIncOp(addr string, reqId string) {
	req := &FinalIncReqMsg{
		Id: reqId,
	}
	rpcStub := gIo.cm.NewRpcStub(addr)
	_, err := rpcStub.FinalizeIncOp(context.Background(), req, grpc.WaitForReady(true))

	if err != nil {
		logger.Errorf("Error: %v", err)
		logger.Fatalf("Fails sending FinalizeIncOp request to addr = %s IR reqId = %s", addr, reqId)
	}
}

func (gIo *grpcIo) ProposeConOp(addr string, reqId string, op []byte) *ConReplyMsg {
	req := &ConReqMsg{
		Req: &Request{
			Id: reqId,
			Op: op,
		},
	}
	rpcStub := gIo.cm.NewRpcStub(addr)
	reply, err := rpcStub.ProposeConOp(context.Background(), req, grpc.WaitForReady(true))

	if err != nil {
		logger.Errorf("Error: %v", err)
		logger.Fatalf("Fails sending ProposeConOp request to addr = %s IR reqId = %s", addr, reqId)
	}
	return reply
}

func (gIo *grpcIo) FinalizeConOp(addr string, reqId string, ret string) {
	req := &FinalConReqMsg{
		Id:  reqId,
		Ret: ret,
	}
	rpcStub := gIo.cm.NewRpcStub(addr)
	_, err := rpcStub.FinalizeConOp(context.Background(), req, grpc.WaitForReady(true))

	if err != nil {
		logger.Errorf("Error: %v", err)
		logger.Fatalf("Fails sending FinalizeConOp request to addr = %s IR reqId = %s", addr, reqId)
	}
}

func (gIo *grpcIo) Probe(addr string) *ProbeReply {
	req := &ProbeReq{}
	rpcStub := gIo.cm.NewRpcStub(addr)
	reply, err := rpcStub.Probe(context.Background(), req, grpc.WaitForReady(true))
	if err != nil {
		logger.Errorf("Error: %v", err)
		logger.Fatalf("Fails sending probing request to addr = %s", addr)
	}
	return reply
}

func (gIo *grpcIo) Test(addr string) {
	req := &TestMsg{}
	rpcStub := gIo.cm.NewRpcStub(addr)
	_, err := rpcStub.Test(context.Background(), req, grpc.WaitForReady(true))
	if err != nil {
		logger.Errorf("Error: %v", err)
		logger.Fatalf("Fails sending test request to addr = %s", addr)
	}
}
