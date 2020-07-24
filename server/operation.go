package server

import (
	"Carousel-GTS/rpc"
)

type Operation interface {
	Execute(storage *Storage)
}

type CoordinatorOperation interface {
	Execute(coordinator *Coordinator)
}

type LockingOp interface {
	ReadAndPrepareOp
	executeFromQueue(storage *Storage) bool
}

type PriorityOp interface {
	LockingOp

	setSelfAbort()

	GetAllWriteKeys() map[string]bool
	GetAllReadKeys() map[string]bool
	GetAllKeys() map[string]bool
	IsSelfAbort() bool
}

type ReadAndPrepareOp interface {
	Operation
	GetReadKeys() map[string]bool
	GetWriteKeys() map[string]bool
	GetTxnId() string
	GetPriority() bool
	GetReadReply() *rpc.ReadAndPrepareReply
	GetReadRequest() *rpc.ReadAndPrepareRequest
	GetCoordinatorPartitionId() int
	GetClientId() string
	GetKeyMap() map[string]bool
	GetTimestamp() int64
	SetReadReply(reply *rpc.ReadAndPrepareReply)
	SetReadKeyAvailable(key string)
	SetWriteKeyAvailable(key string)
	IsReadKeyAvailable(key string) bool
	IsWriteKeyAvailable(key string) bool
	SetPassTimestamp()
	IsPassTimestamp() bool
	SetIndex(i int)
	GetIndex() int

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
	return NewApplyPrepareReplicationMsgOCC(msg)
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

type PriorityOperationCreator struct {
	server *Server
}

func NewPriorityOperationCreator(server *Server) *PriorityOperationCreator {
	g := &PriorityOperationCreator{server: server}
	return g
}

func (g PriorityOperationCreator) createReadAndPrepareOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	if request.Txn.HighPriority {
		return NewReadAndPrepareHighPriority(request, g.server)
	} else {
		return NewReadAndPrepareLowPriority(request, g.server)
	}
}

func (g PriorityOperationCreator) createReadAndPrepareOpWithReplicationMsg(msg *ReplicationMsg) ReadAndPrepareOp {
	return NewReadAndPreparePriorityWithReplicatedMsg(msg)
}

func (g PriorityOperationCreator) createReadOnlyOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	if request.Txn.HighPriority {
		return NewReadOnlyHighPriority(request, g.server)
	} else {
		return NewReadOnlyLowPriority(request, g.server)
	}
}

func (g PriorityOperationCreator) createApplyPrepareResultReplicationOp(msg *ReplicationMsg) Operation {
	return NewApplyPrepareReplicationMsgGTS(msg)
}

func (g PriorityOperationCreator) createAbortOp(abortRequest *rpc.AbortRequest) Operation {
	return NewAbortGTS(abortRequest)
}

func (g PriorityOperationCreator) createCommitOp(request *rpc.CommitRequest) Operation {
	return NewCommitGTS(request)
}

func (g PriorityOperationCreator) createApplyCommitResultReplicationOp(msg *ReplicationMsg) Operation {
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
	return NewApplyPrepareReplicationMsgTwoPL(msg)
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

type TOOperationCreator struct {
	server *Server
}

func NewTOOperationCreator(server *Server) *TOOperationCreator {
	return &TOOperationCreator{server: server}
}

func (t TOOperationCreator) createReadAndPrepareOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	return NewReadAndPrepareTO(request)
}

func (t TOOperationCreator) createReadAndPrepareOpWithReplicationMsg(msg *ReplicationMsg) ReadAndPrepareOp {
	return NewReadAndPrepareTOWithReplicationMsg(msg)
}

func (t TOOperationCreator) createReadOnlyOp(request *rpc.ReadAndPrepareRequest) ReadAndPrepareOp {
	return NewReadOnlyTO(request)
}

func (t TOOperationCreator) createApplyPrepareResultReplicationOp(msg *ReplicationMsg) Operation {
	return NewApplyPrepareReplicationMsgTO(msg)
}

func (t TOOperationCreator) createAbortOp(abortRequest *rpc.AbortRequest) Operation {
	return NewAbortTO(abortRequest)
}

func (t TOOperationCreator) createCommitOp(request *rpc.CommitRequest) Operation {
	return NewCommitTO(request)
}

func (t TOOperationCreator) createApplyCommitResultReplicationOp(msg *ReplicationMsg) Operation {
	return NewApplyCommitResultTO(msg)
}
