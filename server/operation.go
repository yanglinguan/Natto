package server

import (
	"Carousel-GTS/rpc"
)

type Operation interface {
	Execute(storage *Storage)
}

type ScheduleOperation interface {
	Schedule(scheduler *Scheduler)
}

type CoordinatorOperation interface {
	Execute(coordinator *Coordinator)
}

type GTSOp interface {
	ReadAndPrepareOp
	executeFromQueue(storage *Storage) bool

	setIndex(i int)
	setSelfAbort()

	GetKeyMap() map[string]bool
	GetAllWriteKeys() map[string]bool
	GetAllReadKeys() map[string]bool
}

type ReadAndPrepareOp interface {
	Operation
	ScheduleOperation
	GetReadKeys() []string
	GetWriteKeys() []string
	GetTxnId() string
	GetPriority() bool
	GetReadReply() *rpc.ReadAndPrepareReply
	GetReadRequest() *rpc.ReadAndPrepareRequest
	GetCoordinatorPartitionId() int
	GetTimestamp() int64
	SetReadReply(reply *rpc.ReadAndPrepareReply)

	BlockClient()
	UnblockClient()
}

type OperationCreator interface {
	createReadAndPrepareOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp
	createReadAndPrepareOpWithReplicationMsg(msg ReplicationMsg) ReadAndPrepareOp
	createReadOnlyOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp
	createApplyPrepareResultReplicationOp(msg ReplicationMsg) Operation
	createAbortOp(abortRequest *rpc.AbortRequest) Operation
	createCommitOp(commitRequest *rpc.CommitRequest) Operation
	createApplyCommitResultReplicationOp(msg ReplicationMsg) Operation
}

type OCCOperationCreator struct {
	server *Server
}

func NewOCCOperationCreator(server *Server) *OCCOperationCreator {
	o := &OCCOperationCreator{server: server}
	return o
}

func (o OCCOperationCreator) createReadAndPrepareOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	op := NewReadAndPrepareOCC(request)
	return op
}

func (o OCCOperationCreator) createReadAndPrepareOpWithReplicationMsg(msg ReplicationMsg) ReadAndPrepareOp {
	return NewReadAndPrepareOCCWithReplicationMsg(msg)
}

func (o OCCOperationCreator) createApplyPrepareResultReplicationOp(msg ReplicationMsg) Operation {
	return NewOCCApplyPrepareReplicationMsg(msg)
}

func (o OCCOperationCreator) createAbortOp(abortRequest *rpc.AbortRequest) Operation {
	return NewAbortOCC(abortRequest)
}

func (o OCCOperationCreator) createCommitOp(request *rpc.CommitRequest) Operation {
	return NewCommitOCC(request)
}

func (o OCCOperationCreator) createApplyCommitResultReplicationOp(msg ReplicationMsg) Operation {
	return NewApplyCommitResultOCC(msg)
}

func (o OCCOperationCreator) createReadOnlyOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	return NewReadOnlyOCC(request)
}

type GTSOperationCreator struct {
	server *Server
}

func NewGTSOperationCreator(server *Server) *GTSOperationCreator {
	g := &GTSOperationCreator{server: server}
	return g
}

func (g GTSOperationCreator) createReadAndPrepareOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	op := NewReadAndPrepareGTS(request, g.server)
	return op
}

func (g GTSOperationCreator) createReadAndPrepareOpWithReplicationMsg(msg ReplicationMsg) ReadAndPrepareOp {
	return NewReadAndPrepareGTSWithReplicatedMsg(msg, g.server)
}

func (g GTSOperationCreator) createReadOnlyOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	return NewReadOnlyGTS(request, g.server)
}

func (g GTSOperationCreator) createApplyPrepareResultReplicationOp(msg ReplicationMsg) Operation {
	return NewGTSApplyPrepareReplicationMsg(msg)
}

func (g GTSOperationCreator) createAbortOp(abortRequest *rpc.AbortRequest) Operation {
	return NewAbortGTS(abortRequest)
}

func (g GTSOperationCreator) createCommitOp(request *rpc.CommitRequest) Operation {
	return NewCommitGTS(request)
}

func (g GTSOperationCreator) createApplyCommitResultReplicationOp(msg ReplicationMsg) Operation {
	return NewApplyCommitResultGTS(msg)
}
