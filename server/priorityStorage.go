package server

import (
	"Carousel-GTS/rpc"
	"Carousel-GTS/utils"
	log "github.com/sirupsen/logrus"
	"time"
)

func (s *Storage) hasWaitingTxn(op LockingOp) bool {
	return s.kvStore.HasWaitingTxn(op)
}

func (s *Storage) checkReorderConditionPreparedTxn(txnList map[string]*ReadAndPrepareHighPriority) bool {
	if s.server.config.Popular() == 0 {
		return false
	}

	for _, op := range txnList {
		conflict := make(map[string]bool)
		for rk := range op.GetReadKeys() {
			for txn := range s.kvStore.GetTxnHoldWrite(rk) {
				conflict[txn] = true
			}
		}

		for wk := range op.GetWriteKeys() {
			for txn := range s.kvStore.GetTxnHoldRead(wk) {
				conflict[txn] = true
			}
			for txn := range s.kvStore.GetTxnHoldWrite(wk) {
				conflict[txn] = true
			}
		}

		ok := false
		for txn := range conflict {
			txnInfo := s.txnStore[txn].readAndPrepareRequestOp.(*ReadAndPrepareHighPriority)
			for key := range txnInfo.otherPartitionKeys {
				keyInt := utils.ConvertToInt(key)
				if keyInt <= s.server.config.Popular() {
					ok = true
					break
				}
			}
		}

		if !ok {
			return false
		}

	}

	return true
}

func (s *Storage) checkReorderCondition(op *ReadAndPrepareHighPriority) bool {
	conflictTxn := make(map[string]*ReadAndPrepareHighPriority)
	txnId := op.GetTxnId()
	for rk := range op.GetReadKeys() {
		top := s.kvStore.GetNextWaitingTxn(rk)
		if top != nil {
			conflictTxn[top.GetTxnId()] = top.(*ReadAndPrepareHighPriority)
		}

	}

	for wk := range op.GetWriteKeys() {
		top := s.kvStore.GetNextWaitingTxn(wk)
		if top != nil {
			conflictTxn[top.GetTxnId()] = top.(*ReadAndPrepareHighPriority)
		}
	}

	canReorder := true
	for _, txn := range conflictTxn {
		maxPos := 0
		for rk := range txn.GetReadKeys() {
			p := s.kvStore.Position(rk, txnId)
			if p > maxPos {
				maxPos = p
			}
		}
		for wk := range txn.GetWriteKeys() {
			p := s.kvStore.Position(wk, txnId)
			if p > maxPos {
				maxPos = p
			}
		}
		if maxPos < s.server.config.QueuePos() {
			canReorder = false
			break
		}
	}

	if !canReorder {
		return s.checkReorderConditionPreparedTxn(conflictTxn)
	}
	return true
}

func (s *Storage) reorderPrepare(op *ReadAndPrepareHighPriority) {
	if !s.server.config.IsOptimisticReorder() {
		log.Debugf("txn %v does not turn on the optimistic reorder wait", op.txnId)
		s.wait(op)
		return
	}
	if !s.checkReorderCondition(op) {
		log.Debugf("txn %v dose not reorder condition fail", op.txnId)
		s.wait(op)
		return
	}
	log.Debugf("txn %v reorder prepare", op.txnId)
	s.txnStore[op.txnId].status = REORDER_PREPARED
	s.txnStore[op.txnId].canReverse = !op.IsSinglePartition()
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
	s.kvStore.RecordPrepared(op)
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

func (s *Storage) conflictOnOtherPartition(low ReadAndPrepareOp, high ReadAndPrepareOp) bool {
	lowOp := low.(PriorityOp)
	highOp := high.(PriorityOp)
	lowKeys := lowOp.GetOtherPartitionKeys()
	highKeys := highOp.GetOtherPartitionKeys()
	highTxnArrivalTime := highOp.GetEstimatedTimes()
	log.Warnf("low txn %v key %v, high txn %v txn key %v", low.GetTxnId(), lowKeys, high.GetTxnId(), highKeys)
	for key := range lowKeys {
		processTime := lowOp.GetTimestamp()
		if _, exist := highKeys[key]; exist {
			pId := s.server.config.GetPartitionIdByKey(key)
			arrivalTime := highTxnArrivalTime[pId]
			log.Warnf("txn %v and txn %v has conflict on partition %v; high txn arrival %v low txn process %v",
				low.GetTxnId(), high.GetTxnId(), pId, arrivalTime, processTime, lowKeys, highKeys)
			if arrivalTime < processTime {
				return true
			}
		}
	}
	return false
}

func (s *Storage) setForwardPrepare(op *ReadAndPrepareHighPriority, condition map[string]bool) {
	txnId := op.txnId
	if _, exist := s.txnStore[txnId]; !exist {
		log.Fatalf("txn %v txnInfo should be created, and INIT status", txnId)
	}

	prepareResultRequest := &rpc.PrepareResultRequest{
		TxnId:           txnId,
		ReadKeyVerList:  make([]*rpc.KeyVersion, 0),
		WriteKeyVerList: make([]*rpc.KeyVersion, 0),
		PartitionId:     int32(s.server.partitionId),
		PrepareStatus:   int32(s.txnStore[txnId].status),
		Forward:         make([]string, len(condition)),
		Counter:         s.txnStore[txnId].prepareCounter,
	}

	s.txnStore[txnId].prepareCounter++
	s.txnStore[txnId].preparedTime = time.Now()

	i := 0
	for c := range condition {
		prepareResultRequest.Forward[i] = c
		i++
	}
	s.txnStore[txnId].forwardPrepareResultRequest = prepareResultRequest
}

func (s *Storage) setConditionPrepare(op *ReadAndPrepareHighPriority, condition map[string]bool) {
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
		Conditions:      make([]string, len(condition)),
		Counter:         s.txnStore[txnId].prepareCounter,
		EarlyAborts:     make([]string, len(op.lowTxnEarlyAbort)),
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
		prepareResultRequest.Conditions[i] = c
		i++
	}
	i = 0
	for c := range op.lowTxnEarlyAbort {
		prepareResultRequest.EarlyAborts[i] = c
		i++
	}

	s.txnStore[txnId].conditionalPrepareResultRequest = prepareResultRequest
}

func (s *Storage) canConditionalPrepare(txnHold ReadAndPrepareOp, txn ReadAndPrepareOp) bool {
	if txnHold.GetPriority() {
		log.Debugf("txn %v cannot conditional prepare because key %v hold by %v for write", txn.GetTxnId(), txnHold.GetTxnId())
		return false
	}
	if s.outTimeWindow(txnHold, txn) {
		log.Debugf("txn %v cannot conditional prepare because txn %v out of the time window", txn.GetTxnId(), txnHold.GetTxnId())
		return false
	}

	if !s.conflictOnOtherPartition(txnHold, txn) {
		log.Debugf("txn %v cannot conditional prepare because txn %v does not conflict with txn %v on other partitions", txn.GetTxnId(), txnHold.GetTxnId())
		return false
	}
	return true
}

func (s *Storage) checkConditionTxn(op *ReadAndPrepareHighPriority) (bool, map[string]bool) {
	// check if there is high priority txn hold keys
	lowTxnList := make(map[string]bool)
	for rk := range op.GetReadKeys() {
		for txnId := range s.kvStore.GetTxnHoldWrite(rk) {
			if !s.canConditionalPrepare(s.txnStore[txnId].readAndPrepareRequestOp, op) {
				return false, nil
			}
			lowTxnList[txnId] = true
		}
	}

	for wk := range op.GetWriteKeys() {
		for txnId := range s.kvStore.GetTxnHoldRead(wk) {
			if !s.canConditionalPrepare(s.txnStore[txnId].readAndPrepareRequestOp, op) {
				return false, nil
			}
			lowTxnList[txnId] = true
		}

		for txnId := range s.kvStore.GetTxnHoldWrite(wk) {
			if !s.canConditionalPrepare(s.txnStore[txnId].readAndPrepareRequestOp, op) {
				return false, nil
			}
			lowTxnList[txnId] = true
		}
	}

	return true, lowTxnList
}

func (s *Storage) highHold(op *ReadAndPrepareHighPriority) bool {
	if !s.server.config.ForwardReadToCoord() {
		return false
	}

	for rk := range op.GetReadKeys() {
		for txnId := range s.kvStore.GetTxnHoldWrite(rk) {
			if !s.txnStore[txnId].readAndPrepareRequestOp.GetPriority() {
				log.Debugf("txn %v cannot conditional prepare because key %v hold by %v for write", op.txnId, txnId)
				return false
			}
		}
	}

	for wk := range op.GetWriteKeys() {
		for txnId := range s.kvStore.GetTxnHoldRead(wk) {
			if !s.txnStore[txnId].readAndPrepareRequestOp.GetPriority() {
				log.Debugf("txn %v cannot conditional prepare because key %v hold by %v for read", op.txnId, txnId)
				return false
			}
		}

		for txnId := range s.kvStore.GetTxnHoldWrite(wk) {
			if !s.txnStore[txnId].readAndPrepareRequestOp.GetPriority() {
				log.Debugf("txn %v cannot conditional prepare because key %v hold by %v for write", op.txnId, txnId)
				return false
			}
		}
	}
	return true
}

func (s *Storage) forwardPrepare(op *ReadAndPrepareHighPriority) {
	if !s.highHold(op) {
		return
	}
	s.dependGraph.AddNode(op.txnId, op.keyMap)
	s.forwardToCoordinator(op)

	parent := s.dependGraph.GetParent(op.txnId)

	s.txnStore[op.txnId].status = FORWARD_PREPARED
	s.txnStore[op.txnId].isForwardPrepare = true
	s.kvStore.RecordPrepared(op)
	s.setForwardPrepare(op, parent)
	s.replicatePreparedResult(op.txnId)
}

func (s *Storage) forwardToCoordinator(op *ReadAndPrepareHighPriority) {
	parent := s.dependGraph.GetParent(op.txnId)

	// coorId -> [[txnId], [keys]]
	coorServerId := make(map[int][][]string)
	idx := make(map[int][]int32)
	// dependent txn may have the same coordinator
	// merge to one request
	for txnId := range parent {
		coorPId := s.txnStore[txnId].readAndPrepareRequestOp.GetCoordinatorPartitionId()
		serverId := s.server.config.GetLeaderIdByPartitionId(coorPId)
		if _, exist := coorServerId[serverId]; !exist {
			coorServerId[serverId] = make([][]string, 2)
			coorServerId[serverId][0] = make([]string, 0)
			coorServerId[serverId][1] = make([]string, 0)
		}

		keyNum := 0
		for key := range s.txnStore[txnId].readAndPrepareRequestOp.GetWriteKeys() {
			if _, exist := op.GetReadKeys()[key]; exist {
				coorServerId[serverId][1] = append(coorServerId[serverId][1], key)
				keyNum++
			}
		}
		if keyNum == 0 {
			continue
		}
		coorServerId[serverId][0] = append(coorServerId[serverId][0], txnId)
		if _, exist := idx[serverId]; !exist {
			idx[serverId] = make([]int32, 0)
		}
		idx[serverId] = append(idx[serverId], int32(keyNum))
	}
	thisCoorPId := s.txnStore[op.txnId].readAndPrepareRequestOp.GetCoordinatorPartitionId()
	thisCoor := s.server.config.GetLeaderIdByPartitionId(thisCoorPId)
	log.Debugf("txn %v forward read to coord %v", op.txnId, coorServerId)
	for coorId, parentList := range coorServerId {
		if len(parentList[0]) == 0 {
			continue
		}
		request := &rpc.ForwardReadToCoordinator{
			TxnId:      op.txnId,
			ClientId:   op.GetClientId(),
			CoorId:     int32(thisCoor),
			ParentTxns: parentList[0],
			KeyList:    parentList[1],
			Idx:        idx[coorId],
		}
		log.Debugf("txn %v forward request txn %v 's coord %v , keys: %v , idx: %v ",
			op.txnId, parentList[0], parentList[1], idx[coorId])
		sender := NewForwardReadRequestToCoordinatorSender(request, coorId, s.server)
		go sender.Send()
	}

}

func (s *Storage) conditionalPrepare(op *ReadAndPrepareHighPriority) bool {
	if !s.server.config.IsConditionalPrepare() {
		log.Debugf("txn %v does not turn on conditional Prepare wait", op.txnId)
		s.wait(op)
		return false
	}

	prepare, lowTxnList := s.checkConditionTxn(op)
	if !prepare {
		s.wait(op)
		return false
	}

	overlapPartition := s.findOverlapPartitionsWithLowPriorityTxn(op.txnId, lowTxnList)
	log.Debugf("txn %v can conditional prepare condition %v", op.txnId, overlapPartition)
	if len(overlapPartition) == 1 {
		if _, exist := overlapPartition[s.server.partitionId]; exist {
			log.Debugf("txn %v condition is it self wait, condition %v",
				op.txnId, overlapPartition)
			s.wait(op)
			return false
		}
	}

	s.txnStore[op.txnId].status = CONDITIONAL_PREPARED
	s.txnStore[op.txnId].isConditionalPrepare = true
	s.kvStore.RecordPrepared(op)
	s.setReadResult(op, -1, false)
	s.setConditionPrepare(op, lowTxnList)
	s.replicatePreparedResult(op.txnId)
	// add to the queue if condition fail it can prepare as usual
	s.wait(op)
	return true
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
			log.Debugf("read key %v hold by %v", rk, txnId)
			if s.txnStore[txnId].prepareResultRequest == nil {
				continue
			}
			if TxnStatus(s.txnStore[txnId].prepareResultRequest.PrepareStatus) != REORDER_PREPARED ||
				!s.txnStore[txnId].canReverse {
				return false, nil
			}
			reorderTxn[txnId] = true
		}
	}

	for wk := range op.GetWriteKeys() {
		for txnId := range s.kvStore.GetTxnHoldWrite(wk) {
			log.Debugf("write key %v write hold by %v", wk, txnId)
			if s.txnStore[txnId].prepareResultRequest == nil {
				continue
			}
			if TxnStatus(s.txnStore[txnId].prepareResultRequest.PrepareStatus) != REORDER_PREPARED ||
				!s.txnStore[txnId].canReverse {
				return false, nil
			}
			reorderTxn[txnId] = true
		}

		for txnId := range s.kvStore.GetTxnHoldRead(wk) {
			log.Debugf("write key %v read hold by %v", wk, txnId)
			if s.txnStore[txnId].prepareResultRequest == nil {
				continue
			}
			if TxnStatus(s.txnStore[txnId].prepareResultRequest.PrepareStatus) != REORDER_PREPARED ||
				!s.txnStore[txnId].canReverse {
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
				(txnInfo.isConditionalPrepare && txnInfo.status == COMMIT) ||
				(txnInfo.isFastPrepare && txnInfo.status == COMMIT) {
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

	op2, ok := s.txnStore[txnId2].readAndPrepareRequestOp.(PriorityOp)
	if !ok {
		log.Fatalf("txn %v cannot convert to read and prepare gts", op.GetTxnId())
	}

	result := make(map[int]bool)
	for key := range op2.GetAllKeys() {
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
