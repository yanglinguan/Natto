package server

import (
	"Carousel-GTS/rpc"
)

type Operation interface {
	Execute(storage *Storage)
}

//type ScheduleOperation interface {
//	Schedule(scheduler *Scheduler)
//}

type CoordinatorOperation interface {
	Execute(coordinator *Coordinator)
}

type LockingOp interface {
	ReadAndPrepareOp
	executeFromQueue(storage *Storage) bool
	GetKeyMap() map[string]bool
	setIndex(i int)
	getIndex() int
}

type GTSOp interface {
	LockingOp
	//ScheduleOperation
	//executeFromQueue(storage *Storage) bool
	Schedule(scheduler *Scheduler)

	//setIndex(i int)
	//getIndex() int

	setSelfAbort()

	//GetKeyMap() map[string]bool
	GetAllWriteKeys() map[string]bool
	GetAllReadKeys() map[string]bool
	//GetTimestamp() int64
	IsPassTimestamp() bool
	IsSelfAbort() bool
}

type ReadAndPrepareOp interface {
	Operation
	//ScheduleOperation
	GetReadKeys() []string
	GetWriteKeys() []string
	GetTxnId() string
	GetPriority() bool
	GetReadReply() *rpc.ReadAndPrepareReply
	GetReadRequest() *rpc.ReadAndPrepareRequest
	GetCoordinatorPartitionId() int
	GetClientId() string
	//GetKeyMap() map[string]bool
	GetTimestamp() int64
	SetReadReply(reply *rpc.ReadAndPrepareReply)
	//IsPassTimestamp() bool
	//IsSelfAbort() bool
	Start(server *Server)

	BlockClient()
	UnblockClient()
}

type OperationCreator interface {
	createReadAndPrepareOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp
	createReadAndPrepareOpWithReplicationMsg(msg *ReplicationMsg) ReadAndPrepareOp
	createReadOnlyOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp
	createApplyPrepareResultReplicationOp(msg *ReplicationMsg) Operation
	createAbortOp(abortRequest *rpc.AbortRequest) Operation
	createCommitOp(commitRequest *rpc.CommitRequest) Operation
	createApplyCommitResultReplicationOp(msg *ReplicationMsg) Operation
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

func (o OCCOperationCreator) createReadAndPrepareOpWithReplicationMsg(msg *ReplicationMsg) ReadAndPrepareOp {
	return NewReadAndPrepareOCCWithReplicationMsg(msg)
}

func (o OCCOperationCreator) createApplyPrepareResultReplicationOp(msg *ReplicationMsg) Operation {
	return NewOCCApplyPrepareReplicationMsg(msg)
}

func (o OCCOperationCreator) createAbortOp(abortRequest *rpc.AbortRequest) Operation {
	return NewAbortOCC(abortRequest)
}

func (o OCCOperationCreator) createCommitOp(request *rpc.CommitRequest) Operation {
	return NewCommitOCC(request)
}

func (o OCCOperationCreator) createApplyCommitResultReplicationOp(msg *ReplicationMsg) Operation {
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

func (g GTSOperationCreator) createReadAndPrepareOpWithReplicationMsg(msg *ReplicationMsg) ReadAndPrepareOp {
	return NewReadAndPrepareGTSWithReplicatedMsg(msg)
}

func (g GTSOperationCreator) createReadOnlyOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	return NewReadOnlyGTS(request, g.server)
}

func (g GTSOperationCreator) createApplyPrepareResultReplicationOp(msg *ReplicationMsg) Operation {
	return NewGTSApplyPrepareReplicationMsg(msg)
}

func (g GTSOperationCreator) createAbortOp(abortRequest *rpc.AbortRequest) Operation {
	return NewAbortGTS(abortRequest)
}

func (g GTSOperationCreator) createCommitOp(request *rpc.CommitRequest) Operation {
	return NewCommitGTS(request)
}

func (g GTSOperationCreator) createApplyCommitResultReplicationOp(msg *ReplicationMsg) Operation {
	return NewApplyCommitResultGTS(msg)
}

type TwoPLOperationCreator struct {
	server *Server
}

func NewTwoPLOperationCreator(server *Server) *TwoPLOperationCreator {
	return &TwoPLOperationCreator{server: server}
}

func (l TwoPLOperationCreator) createReadAndPrepareOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	return NewReadAndPrepareLock2PL(request)
}

func (l TwoPLOperationCreator) createReadAndPrepareOpWithReplicationMsg(msg *ReplicationMsg) ReadAndPrepareOp {
	return NewReadAndPrepare2PLWithReplicationMsg(msg)
}

func (l TwoPLOperationCreator) createReadOnlyOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	return NewReadOnly2PL(request)
}

func (l TwoPLOperationCreator) createApplyPrepareResultReplicationOp(msg *ReplicationMsg) Operation {
	return NewTwoPLApplyPrepareReplicationMsg(msg)
}

func (l TwoPLOperationCreator) createAbortOp(abortRequest *rpc.AbortRequest) Operation {
	return NewAbort2PL(abortRequest)
}

func (l TwoPLOperationCreator) createCommitOp(request *rpc.CommitRequest) Operation {
	return NewCommit2PL(request)
}

func (l TwoPLOperationCreator) createApplyCommitResultReplicationOp(msg *ReplicationMsg) Operation {
	return NewApplyCommitResult2PL(msg)
}
