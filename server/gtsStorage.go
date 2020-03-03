package server

import (
	"Carousel-GTS/rpc"
	"container/list"
	log "github.com/sirupsen/logrus"
)

type GTSStorage struct {
	kvStore                map[string]*ValueVersion
	server                 *Server
	txnStore               map[string]*TxnInfo
	committed              int
	waitPrintStatusRequest []*PrintStatusRequestOp
	totalCommit            int
}

func NewGTSStorage(server *Server) *GTSStorage {
	s := &GTSStorage{
		kvStore:                make(map[string]*ValueVersion),
		server:                 server,
		txnStore:               make(map[string]*TxnInfo),
		waitPrintStatusRequest: make([]*PrintStatusRequestOp, 0),
		committed:              0,
	}

	return s
}

func (s *GTSStorage) LoadKeys(keys []string) {
	for _, key := range keys {
		s.kvStore[key] = &ValueVersion{
			Value:            key,
			Version:          0,
			WaitingOp:        list.New(),
			WaitingItem:      make(map[string]*list.Element),
			PreparedTxnRead:  make(map[string]bool),
			PreparedTxnWrite: make(map[string]bool),
		}
	}
}

func (s *GTSStorage) Commit(op *CommitRequestOp) {

	txnId := op.request.TxnId
	log.Infof("COMMITTED: %v", txnId)
	if txnInfo, exist := s.txnStore[txnId]; !exist || txnInfo.status != PREPARED {
		log.WithFields(log.Fields{
			"txnId":  txnId,
			"status": txnInfo.status,
		}).Fatal("txn should be prepared before commit")
	}

	for _, rk := range op.request.ReadKeyVerList {
		delete(s.kvStore[rk.Key].PreparedTxnRead, txnId)
	}

	for _, kv := range op.request.WriteKeyValList {
		delete(s.kvStore[kv.Key].PreparedTxnWrite, txnId)
		s.kvStore[kv.Key].Value = kv.Value
		s.kvStore[kv.Key].Version++
		s.checkPrepare(kv.Key)
	}
	s.txnStore[txnId].status = COMMIT
	s.txnStore[txnId].receiveFromCoordinator = true
	s.txnStore[txnId].commitOrder = s.committed
	s.committed++
	op.wait <- true
	s.print()
}

func (s *GTSStorage) selfAbort(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	if txnInfo, exist := s.txnStore[txnId]; exist {
		switch txnInfo.status {
		case ABORT:
			log.WithFields(log.Fields{
				"txnId":  txnId,
				"status": txnInfo.status,
			}).Debugln("txn is already abort by coordinator")

			break
		default:
			log.WithFields(log.Fields{
				"txnId":  txnId,
				"status": txnInfo.status,
			}).Fatalln("error: txn status should be abort")
			break
		}
	} else {
		if int(op.request.Txn.CoordPartitionId) == s.server.partitionId {
			s.server.coordinator.Wait2PCResultTxn <- op
		}
		log.Infof("ABORT: %v (self abort)", txnId)

		s.txnStore[txnId] = &TxnInfo{
			readAndPrepareRequestOp: op,
			status:                  ABORT,
			receiveFromCoordinator:  false,
		}
	}

}

func (s *GTSStorage) coordinatorAbort(request *rpc.AbortRequest) {

	txnId := request.TxnId

	if txnInfo, exist := s.txnStore[txnId]; exist {
		switch txnInfo.status {
		case ABORT:
			log.WithFields(log.Fields{
				"txnId":  txnId,
				"status": txnInfo.status,
			}).Debugln("txn is already abort it self")

			txnInfo.receiveFromCoordinator = true
			break
		case COMMIT:
			log.WithFields(log.Fields{
				"txnId":  txnId,
				"status": txnInfo.status,
			}).Fatalln("txn is already committed")
			break
		default:
			log.Infof("ABORT: %v (coordinator)", txnId)

			for rk, isPrepared := range txnInfo.readAndPrepareRequestOp.readKeyMap {
				if isPrepared {
					delete(s.kvStore[rk].PreparedTxnRead, txnId)
				}
			}

			for wk, isPrepared := range txnInfo.readAndPrepareRequestOp.writeKeyMap {
				if isPrepared {
					delete(s.kvStore[wk].PreparedTxnWrite, txnId)
				}
			}

			for key := range txnInfo.readAndPrepareRequestOp.keyMap {
				if _, exist := s.kvStore[key].WaitingItem[txnId]; exist {
					// if in the queue, then remove from the queue
					s.kvStore[key].WaitingOp.Remove(s.kvStore[key].WaitingItem[txnId])
					delete(s.kvStore[key].WaitingItem, txnId)
				} else {
					// otherwise, check if the top of the queue can prepare
					s.checkPrepare(key)
				}
			}

			if s.txnStore[txnId].readAndPrepareRequestOp != nil {
				s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
			}

			break
		}
	} else {
		log.WithFields(log.Fields{
			"txnId": txnId,
		}).Debugln("txn does not receive a readAndPrepareRequest, create txnInfo")

		s.txnStore[txnId] = &TxnInfo{
			readAndPrepareRequestOp: nil,
			status:                  ABORT,
			receiveFromCoordinator:  true,
		}
	}
}

func (s *GTSStorage) Abort(op *AbortRequestOp) {
	if op.isFromCoordinator {
		s.coordinatorAbort(op.abortRequest)
	} else {
		s.selfAbort(op.request)
		s.setReadResult(op.request)
		op.sendToCoordinator = !s.txnStore[op.request.request.Txn.TxnId].receiveFromCoordinator
		if op.sendToCoordinator {
			s.setPrepareResult(op.request, ABORT)
		}
	}
}

func (s *GTSStorage) checkPrepare(key string) {

	for s.kvStore[key].WaitingOp.Len() != 0 {
		e := s.kvStore[key].WaitingOp.Front()
		op := e.Value.(*ReadAndPrepareOp)
		txnId := op.request.Txn.TxnId
		if s.isAborted(txnId) {
			s.kvStore[key].WaitingOp.Remove(e)
			continue
		}

		canPrepareThisKey := true
		if isPrepared, exist := op.readKeyMap[key]; exist && !isPrepared {
			if !s.hasConflict(key, txnId, READ, true) {
				op.RecordPreparedKey(key, READ)
				s.kvStore[key].PreparedTxnRead[txnId] = true
			} else {
				log.Debugf("txn %v cannot read prepare key %v", txnId, key)
				canPrepareThisKey = false
			}
		}

		if isPrepared, exist := op.writeKeyMap[key]; exist && !isPrepared {
			if !s.hasConflict(key, txnId, WRITE, true) {
				op.RecordPreparedKey(key, WRITE)
				s.kvStore[key].PreparedTxnWrite[txnId] = true
			} else {
				log.Debugf("txn %v cannot write prepare key %v", txnId, key)
				canPrepareThisKey = false
			}
		}

		if !canPrepareThisKey {
			break
		}

		s.kvStore[key].WaitingOp.Remove(e)

		if op.IsPrepared() {
			s.setPrepareResult(op, PREPARED)
		}
	}
}

func (s *GTSStorage) isAborted(txnId string) bool {
	if txnInfo, exist := s.txnStore[txnId]; exist && txnInfo.status == ABORT {
		return true
	}
	return false
}

func (s *GTSStorage) setReadResult(op *ReadAndPrepareOp) {
	op.reply = &rpc.ReadAndPrepareReply{
		KeyValVerList: make([]*rpc.KeyValueVersion, 0),
	}
	for _, rk := range op.request.Txn.ReadKeyList {
		keyValueVersion := &rpc.KeyValueVersion{
			Key:     rk,
			Value:   s.kvStore[rk].Value,
			Version: s.kvStore[rk].Version,
		}
		op.reply.KeyValVerList = append(op.reply.KeyValVerList, keyValueVersion)
	}

	op.wait <- true
}

func (s *GTSStorage) prepareResult(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	if txnInfo, exist := s.txnStore[txnId]; !exist {
		log.WithFields(log.Fields{
			"txnId":   txnId,
			"txnInfo": txnInfo != nil,
		}).Fatalln("txnInfo should be created, and INIT status")
	}

	s.setReadResult(op)

	op.prepareResult = &rpc.PrepareResultRequest{
		TxnId:           txnId,
		ReadKeyVerList:  make([]*rpc.KeyVersion, 0),
		WriteKeyVerList: make([]*rpc.KeyVersion, 0),
		PartitionId:     int32(s.server.partitionId),
		PrepareStatus:   PREPARED,
	}
	for _, rk := range op.request.Txn.ReadKeyList {
		op.prepareResult.ReadKeyVerList = append(op.prepareResult.ReadKeyVerList, &rpc.KeyVersion{
			Key:     rk,
			Version: s.kvStore[rk].Version,
		})
	}

	for _, wk := range op.request.Txn.WriteKeyList {
		op.prepareResult.WriteKeyVerList = append(op.prepareResult.WriteKeyVerList, &rpc.KeyVersion{
			Key:     wk,
			Version: s.kvStore[wk].Version,
		})
	}

	s.txnStore[txnId].status = PREPARED
	op.sendToCoordinator = true

	// unblock to send the result back to client

	// ready to send the coordinator
	s.server.executor.PrepareResult <- &PrepareResultOp{
		Request:          op.prepareResult,
		CoordPartitionId: int(op.request.Txn.CoordPartitionId),
	}
}

func (s *GTSStorage) setPrepareResult(op *ReadAndPrepareOp, status TxnStatus) {

	switch status {
	case PREPARED:
		log.Infof("PREPARED %v", op.request.Txn.TxnId)
		s.prepareResult(op)
	case ABORT:
		log.Infof("ABORT %v", op.request.Txn.TxnId)
		op.prepareResult = &rpc.PrepareResultRequest{
			TxnId:           op.request.Txn.TxnId,
			ReadKeyVerList:  make([]*rpc.KeyVersion, 0),
			WriteKeyVerList: make([]*rpc.KeyVersion, 0),
			PartitionId:     int32(s.server.partitionId),
			PrepareStatus:   ABORT,
		}
		s.server.executor.PrepareResult <- &PrepareResultOp{
			Request:          op.prepareResult,
			CoordPartitionId: int(op.request.Txn.CoordPartitionId),
		}
	}

}

// return true if has Conflict
func (s *GTSStorage) hasConflict(key string, txnId string, keyType KeyType, isTop bool) bool {
	switch keyType {
	case READ:
		return len(s.kvStore[key].PreparedTxnWrite) > 0 || (!isTop && s.kvStore[key].WaitingOp.Len() > 0)
	case WRITE:
		if !isTop && s.kvStore[key].WaitingOp.Len() > 0 {
			return true
		} else {
			if len(s.kvStore[key].PreparedTxnWrite) > 0 || len(s.kvStore[key].PreparedTxnRead) > 1 {
				return true
			}
			if len(s.kvStore[key].PreparedTxnRead) == 1 && !s.kvStore[key].PreparedTxnRead[txnId] {
				return true
			}
			return false
		}
	default:
		log.Fatalln("the key only has type READ or Write")
		return true
	}
}

func (s *GTSStorage) Prepare(op *ReadAndPrepareOp) {
	log.Infof("PROCESSING txn %v", op.request.Txn.TxnId)
	txnId := op.request.Txn.TxnId
	if s.isAborted(txnId) {
		log.Infof("txn %v is already aborted", op.request.Txn.TxnId)
		return
	}

	if int(op.request.Txn.CoordPartitionId) == s.server.partitionId {
		s.server.coordinator.Wait2PCResultTxn <- op
	}

	if op.request.IsNotParticipant {
		return
	}

	s.txnStore[txnId] = &TxnInfo{
		readAndPrepareRequestOp: op,
		status:                  INIT,
		receiveFromCoordinator:  false,
	}

	notPreparedKey := make(map[string]bool)
	for rk := range op.readKeyMap {
		log.Debugf("txn %v read key %v", txnId, rk)
		if !s.hasConflict(rk, txnId, READ, false) {
			op.RecordPreparedKey(rk, READ)
			s.kvStore[rk].PreparedTxnRead[txnId] = true
		} else {
			log.Debugf("txn %v wait read key %v", txnId, rk)
			notPreparedKey[rk] = true
		}
	}

	for wk := range op.writeKeyMap {
		log.Debugf("txn %v write key %v", txnId, wk)
		if !s.hasConflict(wk, txnId, WRITE, false) {
			op.RecordPreparedKey(wk, WRITE)
			s.kvStore[wk].PreparedTxnWrite[txnId] = true
		} else {
			log.Debugf("txn %v wait write key %v", txnId, wk)
			notPreparedKey[wk] = true
		}
	}

	if !op.IsPrepared() {
		for key := range notPreparedKey {
			item := s.kvStore[key].WaitingOp.PushBack(op)
			s.kvStore[key].WaitingItem[txnId] = item
		}
		return
	}

	s.setPrepareResult(op, PREPARED)
}

func (s *GTSStorage) PrintStatus(op *PrintStatusRequestOp) {
	s.waitPrintStatusRequest = append(s.waitPrintStatusRequest, op)
	s.totalCommit += op.committedTxn
	s.print()
}

func (s *GTSStorage) print() {
	if len(s.waitPrintStatusRequest) == s.server.config.GetTotalClient() && s.totalCommit == s.committed {
		printCommitOrder(s.txnStore, s.committed, s.server.serverId)
		printModifiedData(s.kvStore, s.server.serverId)
		for _, printOp := range s.waitPrintStatusRequest {
			printOp.wait <- true
		}
	}
}
