package server

import "Carousel-GTS/rpc"

type PartitionStatus struct {
	status          TxnStatus
	fastPrepareUsed bool
	isFastPrepare   bool
	prepareResult   *rpc.PrepareResultRequest

	reorderAgreementReceived map[string]bool
	counter                  int32
}

func NewPartitionStatus(
	status TxnStatus,
	fastPrepareUsed bool,
	prepareResult *rpc.PrepareResultRequest) *PartitionStatus {

	p := &PartitionStatus{
		status:                   status,
		isFastPrepare:            fastPrepareUsed,
		fastPrepareUsed:          fastPrepareUsed,
		prepareResult:            prepareResult,
		reorderAgreementReceived: make(map[string]bool),
	}

	if prepareResult != nil {
		p.counter = prepareResult.Counter
	}

	return p
}
