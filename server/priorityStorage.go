package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
	"time"
)

func (s *Storage) hasWaitingTxn(op LockingOp) bool {
	return s.kvStore.HasWaitingTxn(op)
}

func (s *Storage) reorderPrepare(op *ReadAndPrepareHighPriority) {
	if !s.server.config.IsOptimisticReorder() {
		log.Debugf("txn %v does not turn on the optimistic reorder wait", op.txnId)
		s.wait(op)
		return
	}
	log.Debugf("txn %v reorder prepare", op.txnId)
	s.txnStore[op.txnId].status = REORDER_PREPARED
	s.kvStore.RecordPrepared(op)
	s.setReadResult(op, -1, false)
	s.setPrepareResult(op)
	s.replicatePreparedResult(op.GetTxnId())
}

func (s *Storage) outTimeWindow(low ReadAndPrepareOp, high ReadAndPrepareOp) bool {
	lowGTS := low.(PriorityOp)
	highGTS := low.(PriorityOp)
	duration := time.Duration(highGTS.GetTimestamp() - lowGTS.GetTimestamp())
	return duration > s.server.config.GetTimeWindow()
}

func (s *Storage) reverseReorderPrepare(op *ReadAndPrepareHighPriority, reorderTxn map[string]bool) {
	log.Debugf("txn %v reverser reorder", op.txnId)
	s.txnStore[op.txnId].status = REVERSE_REORDER_PREPARED
	s.setReverseReorderPrepareResult(op, reorderTxn)
	s.replicatePreparedResult(op.txnId)
}

func (s *Storage) setReverseReorderPrepareResult(op *ReadAndPrepareHighPriority,
	reorderTxn map[string]bool) {
	txnId := op.txnId
	if _, exist := s.txnStore[txnId]; !exist {
		log.Fatalf("txn %v txnInfo should be created, and INIT status", txnId)
	}

	if s.txnStore[txnId].prepareResultRequest != nil {
		log.Debugf("txn %v prepare prepareResultRequest is already exist", txnId)
		return
	}
	prepareResultRequest := &rpc.PrepareResultRequest{
		TxnId:           txnId,
		ReadKeyVerList:  make([]*rpc.KeyVersion, 0),
		WriteKeyVerList: make([]*rpc.KeyVersion, 0),
		PartitionId:     int32(s.server.partitionId),
		PrepareStatus:   int32(s.txnStore[txnId].status),
		Reorder:         make([]string, len(reorderTxn)),
		Counter:         s.txnStore[txnId].prepareCounter,
	}
	s.txnStore[txnId].prepareCounter++
	s.txnStore[txnId].preparedTime = time.Now()

	for rk := range op.GetReadKeys() {
		_, version := s.kvStore.Get(rk)
		prepareResultRequest.ReadKeyVerList = append(prepareResultRequest.ReadKeyVerList,
			&rpc.KeyVersion{
				Key:     rk,
				Version: version,
			},
		)
	}

	for wk := range op.GetWriteKeys() {
		_, version := s.kvStore.Get(wk)
		prepareResultRequest.WriteKeyVerList = append(prepareResultRequest.WriteKeyVerList,
			&rpc.KeyVersion{
				Key:     wk,
				Version: version,
			},
		)
	}

	i := 0
	for txn := range reorderTxn {
		prepareResultRequest.Reorder[i] = txn
		i++
	}

	s.txnStore[txnId].prepareResultRequest = prepareResultRequest
}

func (s *Storage) setConditionPrepare(op *ReadAndPrepareHighPriority, condition map[int]bool) {
	txnId := op.txnId
	if _, exist := s.txnStore[txnId]; !exist {
		log.Fatalf("txn %v txnInfo should be created, and INIT status", txnId)
	}

	//if s.txnStore[txnId].prepareResultRequest != nil {
	//	log.Debugf("txn %v prepare prepareResultRequest is already exist", txnId)
	//	return
	//}
	prepareResultRequest := &rpc.PrepareResultRequest{
		TxnId:           txnId,
		ReadKeyVerList:  make([]*rpc.KeyVersion, 0),
		WriteKeyVerList: make([]*rpc.KeyVersion, 0),
		PartitionId:     int32(s.server.partitionId),
		PrepareStatus:   int32(s.txnStore[txnId].status),
		Conditions:      make([]int32, len(condition)),
		Counter:         s.txnStore[txnId].prepareCounter,
	}

	s.txnStore[txnId].prepareCounter++
	s.txnStore[txnId].preparedTime = time.Now()

	for rk := range op.GetReadKeys() {
		_, version := s.kvStore.Get(rk)
		prepareResultRequest.ReadKeyVerList = append(prepareResultRequest.ReadKeyVerList,
			&rpc.KeyVersion{
				Key:     rk,
				Version: version,
			},
		)
	}

	for wk := range op.GetWriteKeys() {
		_, version := s.kvStore.Get(wk)
		prepareResultRequest.WriteKeyVerList = append(prepareResultRequest.WriteKeyVerList,
			&rpc.KeyVersion{
				Key:     wk,
				Version: version,
			},
		)
	}

	i := 0
	for c := range condition {
		prepareResultRequest.Conditions[i] = int32(c)
		i++
	}

	s.txnStore[txnId].conditionalPrepareResultRequest = prepareResultRequest
}

func (s *Storage) checkConditionTxn(op *ReadAndPrepareHighPriority) (bool, map[string]bool) {
	// check if there is high priority txn hold keys
	lowTxnList := make(map[string]bool)
	for rk := range op.GetReadKeys() {
		for txnId := range s.kvStore.GetTxnHoldWrite(rk) {
			if s.txnStore[txnId].readAndPrepareRequestOp.GetPriority() {
				log.Debugf("txn %v cannot conditional prepare because key %v hold by %v for write", op.txnId, txnId)
				return false, nil
			} else if s.outTimeWindow(s.txnStore[txnId].readAndPrepareRequestOp, op) {
				log.Debugf("txn %v cannot conditional prepare because txn %v out of the time window", op.txnId, txnId)
				return false, nil
			}
			lowTxnList[txnId] = true
		}
	}

	for wk := range op.GetWriteKeys() {
		for txnId := range s.kvStore.GetTxnHoldRead(wk) {
			if s.txnStore[txnId].readAndPrepareRequestOp.GetPriority() {
				log.Debugf("txn %v cannot conditional prepare because key %v hold by %v for read", op.txnId, txnId)
				return false, nil
			} else if s.outTimeWindow(s.txnStore[txnId].readAndPrepareRequestOp, op) {
				log.Debugf("txn %v cannot conditional prepare because txn %v out of the time window", op.txnId, txnId)
				return false, nil
			}
			lowTxnList[txnId] = true
		}

		for txnId := range s.kvStore.GetTxnHoldWrite(wk) {
			if s.txnStore[txnId].readAndPrepareRequestOp.GetPriority() {
				log.Debugf("txn %v cannot conditional prepare because key %v hold by %v for write", op.txnId, txnId)
				return false, nil
			} else if s.outTimeWindow(s.txnStore[txnId].readAndPrepareRequestOp, op) {
				log.Debugf("txn %v cannot conditional prepare because txn %v out of the time window", op.txnId, txnId)
				return false, nil
			}
			lowTxnList[txnId] = true
		}
	}

	return true, lowTxnList
}

func (s *Storage) conditionalPrepare(op *ReadAndPrepareHighPriority) {
	if !s.server.config.IsConditionalPrepare() {
		log.Debugf("txn %v does not turn on conditional Prepare wait", op.txnId)
		s.wait(op)
		return
	}

	prepare, lowTxnList := s.checkConditionTxn(op)
	if !prepare {
		s.wait(op)
		return
	}

	overlapPartition := s.findOverlapPartitionsWithLowPriorityTxn(op.txnId, lowTxnList)
	log.Debugf("txn %v can conditional prepare condition %v", op.txnId, overlapPartition)
	if len(overlapPartition) == 1 {
		if _, exist := overlapPartition[s.server.partitionId]; exist {
			log.Debugf("txn %v condition is it self wait, condition %v",
				op.txnId, overlapPartition)
			s.wait(op)
			return
		}
	}

	s.txnStore[op.txnId].status = CONDITIONAL_PREPARED
	s.txnStore[op.txnId].isConditionalPrepare = true
	s.kvStore.RecordPrepared(op)
	s.setReadResult(op, -1, false)
	s.setConditionPrepare(op, overlapPartition)
	s.replicatePreparedResult(op.txnId)
	// add to the queue if condition fail it can prepare as usual
	s.wait(op)
}

func (s *Storage) wait(op LockingOp) {
	log.Debugf("txn %v wait", op.GetTxnId())
	s.txnStore[op.GetTxnId()].status = WAITING
	maxQueueLen := s.kvStore.AddToWaitingList(op)
	s.txnStore[op.GetTxnId()].maxQueueLen = maxQueueLen
}

func (s *Storage) removeFromQueue(op LockingOp) {
	s.kvStore.RemoveFromWaitingList(op)
}

func (s *Storage) checkKeysAvailableFromQueue(op *ReadAndPrepareHighPriority) (bool, map[string]bool) {
	if !s.server.config.IsOptimisticReorder() {
		return s.checkKeysAvailable(op), nil
	}

	reorderTxn := make(map[string]bool)
	for rk := range op.GetReadKeys() {
		for txnId := range s.kvStore.GetTxnHoldWrite(rk) {
			if TxnStatus(s.txnStore[txnId].prepareResultRequest.PrepareStatus) != REORDER_PREPARED {
				return false, nil
			}
			reorderTxn[txnId] = true
		}
	}

	for wk := range op.GetWriteKeys() {
		for txnId := range s.kvStore.GetTxnHoldWrite(wk) {
			if TxnStatus(s.txnStore[txnId].prepareResultRequest.PrepareStatus) != REORDER_PREPARED {
				return false, nil
			}
			reorderTxn[txnId] = true
		}

		for txnId := range s.kvStore.GetTxnHoldRead(wk) {
			if TxnStatus(s.txnStore[txnId].prepareResultRequest.PrepareStatus) != REORDER_PREPARED {
				return false, nil
			}
			reorderTxn[txnId] = true
		}
	}

	return true, reorderTxn
}

// check if there is txn can be prepared when key is released
func (s *Storage) checkPrepare(key string) {
	op := s.kvStore.GetNextWaitingTxn(key)
	for op != nil {
		txnId := op.GetTxnId()
		// skip the aborted txn
		if txnInfo, exist := s.txnStore[txnId]; exist {
			if txnInfo.status.IsAbort() || txnInfo.status == COMMIT ||
				(txnInfo.isConditionalPrepare && txnInfo.status == COMMIT) {
				log.Debugf("txn %v status: %v condition prepare: %v key: %v",
					txnId, txnInfo.status, txnInfo.isConditionalPrepare, key)
				s.kvStore.RemoveFromWaitingList(op)
				op = s.kvStore.GetNextWaitingTxn(key)
				continue
			}
		}

		prepare := op.executeFromQueue(s)
		if !prepare {
			break
		}
		log.Debugf("txn %v prepared from queue", txnId)
		op = s.kvStore.GetNextWaitingTxn(key)
	}
}

// release the keys that txn holds
// check if there is txn can be prepared when keys are released
func (s *Storage) releaseKeyAndCheckPrepare(txnId string) {
	op, ok := s.txnStore[txnId].readAndPrepareRequestOp.(LockingOp)
	if !ok {
		log.Fatalf("txn %v should be readAndPrepareGTS", txnId)
	}
	s.kvStore.ReleaseKeys(op)
	s.kvStore.RemoveFromWaitingList(op)
	for key := range op.GetKeyMap() {
		//s.kvStore.removeFromQueue(op, key)
		log.Debugf("txn %v release key %v check if txn can be prepared", txnId, key)
		s.checkPrepare(key)
	}
}

func (s *Storage) releaseKeyAndCheckPrepareByRePrepare(txnId string) {
	op, ok := s.txnStore[txnId].readAndPrepareRequestOp.(LockingOp)
	if !ok {
		log.Fatalf("txn %v should be readAndPrepareGTS", txnId)
	}
	s.kvStore.ReleaseKeys(op)
	for key := range op.GetKeyMap() {
		//s.kvStore.removeFromQueue(op, key)
		log.Debugf("txn %v release key %v check if txn can be prepared", txnId, key)
		s.checkPrepare(key)
	}
}

func (s *Storage) Release(op LockingOp) {
	for key := range op.GetKeyMap() {
		s.checkPrepare(key)
	}
}

func (s *Storage) overlapPartitions(txnId1 string, txnId2 string) map[int]bool {
	p1 := make(map[int]bool)
	op, ok := s.txnStore[txnId1].readAndPrepareRequestOp.(PriorityOp)
	if !ok {
		log.Fatalf("txn %v cannot convert to read and prepare gts", op.GetTxnId())
	}
	for key := range op.GetAllKeys() {
		pId := s.server.config.GetPartitionIdByKey(key)
		p1[pId] = true
	}
	result := make(map[int]bool)
	for key := range op.GetAllKeys() {
		pId := s.server.config.GetPartitionIdByKey(key)
		if _, exist := p1[pId]; exist {
			result[pId] = true
		}
	}

	log.Debugf("txn %v and txn %v overlap partition %v", txnId1, txnId2, result)

	return result
}

func (s *Storage) findOverlapPartitionsWithLowPriorityTxn(txnId string, conflictLowPriorityTxn map[string]bool) map[int]bool {
	overlapPartition := make(map[int]bool)

	log.Debugf("txn %v conflict low priority txn %v", txnId, conflictLowPriorityTxn)
	for lowTxnId := range conflictLowPriorityTxn {
		pIdMap := s.overlapPartitions(txnId, lowTxnId)
		for pId := range pIdMap {
			overlapPartition[pId] = true
		}
	}

	return overlapPartition
}
