package server

type FastPrepareStatus struct {
	txnId            string
	partitionId      int
	quorum           int
	fastResult       []*FastPrepareRequestOp
	upToDateResult   []*FastPrepareRequestOp
	leaderResult     *FastPrepareRequestOp
	leaderKeyVersion map[string]uint64

	isSuccess bool
	result    TxnStatus
}

func NewFastPrepareStatus(txnId string, partitionId int, quorum int) *FastPrepareStatus {
	f := &FastPrepareStatus{
		txnId:            txnId,
		partitionId:      partitionId,
		quorum:           quorum - 1, // not including leader
		fastResult:       make([]*FastPrepareRequestOp, 0),
		upToDateResult:   make([]*FastPrepareRequestOp, 0),
		leaderResult:     nil,
		leaderKeyVersion: nil,
		isSuccess:        false,
		result:           INIT,
	}

	return f
}

func (f *FastPrepareStatus) setLeaderResult(result *FastPrepareRequestOp) {
	f.leaderResult = result
	f.leaderKeyVersion = make(map[string]uint64)
	for _, kv := range result.request.PrepareResult.ReadKeyVerList {
		f.leaderKeyVersion[kv.Key] = kv.Version
	}
	for _, kv := range result.request.PrepareResult.WriteKeyVerList {
		f.leaderKeyVersion[kv.Key] = kv.Version
	}
	f.upToDateResult = make([]*FastPrepareRequestOp, 0)
	for _, r := range f.fastResult {
		if f.isUpToDate(r) {
			f.upToDateResult = append(f.upToDateResult, r)
		}
	}
}

func (f *FastPrepareStatus) addFastPrepare(result *FastPrepareRequestOp) {
	if result.request.IsLeader {
		f.setLeaderResult(result)
		return
	}

	if f.leaderResult != nil {
		if f.isUpToDate(result) {
			f.upToDateResult = append(f.upToDateResult, result)
		}
	} else {
		f.fastResult = append(f.fastResult, result)
	}
}

func (f *FastPrepareStatus) isFastPathSuccess() (bool, TxnStatus) {
	if f.isSuccess {
		return f.isSuccess, f.result
	}

	if f.leaderResult == nil || len(f.upToDateResult) < f.quorum {
		return false, INIT
	}

	f.isSuccess, f.result = f.isSameDecision()
	return f.isSuccess, f.result
}

func (f *FastPrepareStatus) isSameDecision() (bool, TxnStatus) {
	num := len(f.upToDateResult)
	for _, result := range f.upToDateResult {
		if result.request.PrepareResult.PrepareStatus !=
			f.leaderResult.request.PrepareResult.PrepareStatus {
			num--
			if num < f.quorum {
				return false, INIT
			}
		}
	}

	return true, TxnStatus(f.leaderResult.request.PrepareResult.PrepareStatus)
}

func (f *FastPrepareStatus) isUpToDate(result *FastPrepareRequestOp) bool {
	for _, result := range f.fastResult {
		if result.request.IsLeader {
			continue
		}
		if result.request.RaftTerm != f.leaderResult.request.RaftTerm {
			return false
		}

		for _, kv := range result.request.PrepareResult.ReadKeyVerList {
			if f.leaderKeyVersion[kv.Key] != kv.Version {
				return false
			}
		}

		for _, kv := range result.request.PrepareResult.WriteKeyVerList {
			if f.leaderKeyVersion[kv.Key] != kv.Version {
				return false
			}
		}
	}

	return true
}
