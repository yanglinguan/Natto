package server

import (
	"Carousel-GTS/rpc"
	"time"
)

type TxnStatus int32

const (
	INIT TxnStatus = iota
	WAITING
	CONDITIONAL_PREPARED
	REORDER_PREPARED
	REVERSE_REORDER_PREPARED
	REVERSED
	PREPARED
	COMMIT
	ABORT
)

type AbortReason int32

const (
	NONE AbortReason = iota
	PASSTIME
	CONFLICTHIHG
	READVERSION
	CYCLE
)

type TxnInfo struct {
	readAndPrepareRequestOp          ReadAndPrepareOp
	prepareResultRequest             *rpc.PrepareResultRequest
	status                           TxnStatus
	receiveFromCoordinator           bool
	sendToClient                     bool
	sendToCoordinator                bool
	commitOrder                      int
	waitingTxnKey                    int
	waitingTxnDep                    int
	startTime                        time.Time
	preparedTime                     time.Time
	commitTime                       time.Time
	canReorder                       int
	isFastPrepare                    bool
	inQueue                          bool
	hasWaitingButNoWriteReadConflict bool
	selfAbort                        bool
	abortReason                      AbortReason
	prepareCounter                   int32
}

func NewTxnInfo() *TxnInfo {
	t := &TxnInfo{
		readAndPrepareRequestOp: nil,
		prepareResultRequest:    nil,
		status:                  INIT,
		receiveFromCoordinator:  false,
		sendToClient:            false,
		sendToCoordinator:       false,
		commitOrder:             0,
		waitingTxnKey:           0,
		waitingTxnDep:           0,
		startTime:               time.Now(),
		preparedTime:            time.Now(),
		commitTime:              time.Now(),
		canReorder:              0,
	}

	return t
}
