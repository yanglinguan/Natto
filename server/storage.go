package server

import (
	"container/list"
)

type KeyType int

const (
	READ = iota
	WRITE
)

type TxnStatus int

const (
	INIT = iota
	PREPARED
	COMMIT
	ABORT
)

type TxnInfo struct {
	readAndPrepareRequestOp *ReadAndPrepareOp
	status                  TxnStatus
	receiveFromCoordinator  bool
	commitOrder             int
}

type ValueVersion struct {
	Value            string
	Version          uint64
	WaitingOp        *list.List
	PreparedTxnRead  map[string]bool
	PreparedTxnWrite map[string]bool
}

type Storage interface {
	Prepare(op *ReadAndPrepareOp)
	Commit(op *CommitRequestOp)
	Abort(op *AbortRequestOp)
	LoadKeys(keys []string)
	PrintStatus()
}
