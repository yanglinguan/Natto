package server

import (
	"Carousel-GTS/rpc"
	"fmt"
	"time"
)

type TxnStatus int32

const (
	INIT TxnStatus = iota
	WAITING
	CONDITIONAL_PREPARED
	REORDER_PREPARED
	REVERSE_REORDER_PREPARED
	FORWARD_PREPARED
	PREPARED
	EARLY_ABORT_PREPARED
	COMMIT
	// mark by partition server
	PASS_TIMESTAMP_ABORT
	EARLY_ABORT
	CONFLICT_ABORT
	WAITING_ABORT
	COORDINATOR_ABORT
	WOUND_ABORT
	READTS_ABORT
	WRITETS_ABORT
	// mark by coordinator
	CLIENT_ABORT
	READ_VERSION_ABORT
	CONDITION_ABORT
	FORWARD_ABORT
)

func (t TxnStatus) IsAbort() bool {
	switch t {
	case PASS_TIMESTAMP_ABORT, EARLY_ABORT, CONFLICT_ABORT, WAITING_ABORT,
		READ_VERSION_ABORT, CONDITION_ABORT, CLIENT_ABORT, WOUND_ABORT, FORWARD_ABORT:
		return true
	default:
		return false
	}
}

func (t TxnStatus) IsPrepare() bool {
	switch t {
	case PREPARED, EARLY_ABORT_PREPARED, REVERSE_REORDER_PREPARED, REORDER_PREPARED, CONDITIONAL_PREPARED, FORWARD_PREPARED:
		return true
	default:
		return false
	}
}

func (t TxnStatus) String() string {
	switch t {
	case INIT:
		return "INIT"
	case WAITING:
		return "WAITING"
	case CONDITIONAL_PREPARED:
		return "CONDITIONAL_PREPARED"
	case REORDER_PREPARED:
		return "REORDER_PREPARED"
	case REVERSE_REORDER_PREPARED:
		return "REVERSE_REORDER_PREPARED"
	case PREPARED:
		return "PREPARED"
	case COMMIT:
		return "COMMIT"
	case PASS_TIMESTAMP_ABORT:
		return "PASS_TIMESTAMP_ABORT"
	case EARLY_ABORT:
		return "EARLY_ABORT"
	case CONFLICT_ABORT:
		return "CONFLICT_ABORT"
	case WAITING_ABORT:
		return "WAITING_ABORT"
	case COORDINATOR_ABORT:
		return "COORDINATOR_ABORT"
	case CLIENT_ABORT:
		return "CLIENT_ABORT"
	case READ_VERSION_ABORT:
		return "READ_VERSION_ABORT"
	case CONDITION_ABORT:
		return "CONDITION_ABORT"
	case WOUND_ABORT:
		return "WOUND_ABORT"
	case READTS_ABORT:
		return "READTS_ABORT"
	case WRITETS_ABORT:
		return "WRITETS_ABORT"
	case EARLY_ABORT_PREPARED:
		return "EARLY_ABORT_PREPARED"
	case FORWARD_PREPARED:
		return "FORWARD_PREPARED"
	default:
		return fmt.Sprintf("%d", int(t))
	}
}

type TxnInfo struct {
	readAndPrepareRequestOp          ReadAndPrepareOp
	prepareResultRequest             *rpc.PrepareResultRequest
	conditionalPrepareResultRequest  *rpc.PrepareResultRequest
	forwardPrepareResultRequest      *rpc.PrepareResultRequest
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
	prepareCounter                   int32
	isConditionalPrepare             bool
	isForwardPrepare                 bool
	maxQueueLen                      int
	canReverse                       bool
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
		prepareCounter:          0,
	}

	return t
}
