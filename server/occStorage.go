package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
)

type OccStorage struct {
	kvStore  map[string]*ValueVersion
	txnStore map[string]*TxnInfo

	preparedReadKey  map[string]int
	preparedWriteKey map[string]int

	server *Server

	committed int

	waitPrintStatusRequest []*PrintStatusRequestOp
	totalCommit            int
}

func NewOccStorage(server *Server) *OccStorage {
	o := &OccStorage{
		kvStore:                make(map[string]*ValueVersion),
		txnStore:               make(map[string]*TxnInfo),
		preparedReadKey:        make(map[string]int),
		preparedWriteKey:       make(map[string]int),
		server:                 server,
		waitPrintStatusRequest: make([]*PrintStatusRequestOp, 0),
		committed:              0,
	}

	return o
}

func (s *OccStorage) isAborted(txnId string) bool {
	if txnInfo, exist := s.txnStore[txnId]; exist && txnInfo.status == ABORT {
		return true
	}
	return false
}

func (s *OccStorage) prepareResult(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId

	if txnInfo, exist := s.txnStore[txnId]; !exist || txnInfo.status != INIT {
		log.WithFields(log.Fields{
			"txnId":   txnId,
			"txnInfo": txnInfo != nil,
		}).Fatalln("txnInfo should be created, and INIT status")
	}

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

	// ready to send the coordinator
	s.server.executor.PrepareResult <- &PrepareResultOp{
		Request:          op.prepareResult,
		CoordPartitionId: int(op.request.Txn.CoordPartitionId),
	}
}

func (s *OccStorage) setPrepareResult(op *ReadAndPrepareOp, status TxnStatus) {
	switch status {
	case PREPARED:
		s.prepareResult(op)
	case ABORT:
		op.prepareResult = &rpc.PrepareResultRequest{
			TxnId:           op.request.Txn.TxnId,
			ReadKeyVerList:  nil,
			WriteKeyVerList: nil,
			PartitionId:     int32(s.server.partitionId),
			PrepareStatus:   ABORT,
		}
		s.server.executor.PrepareResult <- &PrepareResultOp{
			Request:          op.prepareResult,
			CoordPartitionId: int(op.request.Txn.CoordPartitionId),
		}
	}
}

func (s *OccStorage) Prepare(op *ReadAndPrepareOp) {
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

	// return to client
	op.wait <- true

	txnId := op.request.Txn.TxnId
	if s.isAborted(txnId) {
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
		status:                  0,
		receiveFromCoordinator:  false,
		commitOrder:             0,
	}

	canPrepare := true
	for key := range op.readKeyMap {
		// read write conflict
		if s.preparedWriteKey[key] > 0 {
			canPrepare = false
			break
		}
	}

	for key := range op.writeKeyMap {
		if !canPrepare || s.preparedReadKey[key] > 0 || s.preparedWriteKey[key] > 0 {
			canPrepare = false
			break
		}
	}

	if canPrepare {
		for _, rk := range op.request.Txn.ReadKeyList {
			s.preparedReadKey[rk]++
		}
		for _, wk := range op.request.Txn.WriteKeyList {
			s.preparedWriteKey[wk]++
		}
		s.setPrepareResult(op, PREPARED)
		return
	}

	s.txnStore[txnId].status = ABORT

	abortOp := NewAbortRequestOp(nil, op, false)
	s.server.executor.AbortTxn <- abortOp

}

func (s *OccStorage) Commit(op *CommitRequestOp) {
	log.Infof("COMMIT %v", op.request.TxnId)
	for _, kv := range op.request.WriteKeyValList {
		log.Debugf("key %v, value %v", kv.Key, kv.Value)
		s.kvStore[kv.Key].Value = kv.Value
		s.kvStore[kv.Key].Version++
		s.preparedWriteKey[kv.Key]--
	}

	for _, kv := range op.request.ReadKeyVerList {
		s.preparedReadKey[kv.Key]--
	}
	s.txnStore[op.request.TxnId].status = COMMIT
	s.txnStore[op.request.TxnId].receiveFromCoordinator = true
	s.txnStore[op.request.TxnId].commitOrder = s.committed
	s.committed++
	op.wait <- true
	s.print()
}

func (s *OccStorage) coordinatorAbort(request *rpc.AbortRequest) {
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
		case PREPARED:
			log.Infof("ABORT %v (coordinator) PREPARED", txnId)
			for rk := range txnInfo.readAndPrepareRequestOp.readKeyMap {
				s.preparedReadKey[rk]--
			}
			for wk := range txnInfo.readAndPrepareRequestOp.writeKeyMap {
				s.preparedWriteKey[wk]--
			}
			txnInfo.receiveFromCoordinator = true
			txnInfo.status = ABORT
			break
		default:
			log.Infof("ABORT %v (coordinator) INIT", txnId)
			txnInfo.receiveFromCoordinator = true
			txnInfo.status = ABORT
			break
		}
	} else {
		log.Infof("ABORT %v (coordinator init txnInfo)", txnId)

		s.txnStore[txnId] = &TxnInfo{
			readAndPrepareRequestOp: nil,
			status:                  ABORT,
			receiveFromCoordinator:  true,
		}
	}
}

func (s *OccStorage) Abort(op *AbortRequestOp) {
	if op.isFromCoordinator {
		s.coordinatorAbort(op.abortRequest)
	} else {
		op.sendToCoordinator = !s.txnStore[op.request.request.Txn.TxnId].receiveFromCoordinator
		if op.sendToCoordinator {
			s.setPrepareResult(op.request, ABORT)
		}
	}
}

func (s *OccStorage) LoadKeys(keys []string) {
	for _, key := range keys {
		s.kvStore[key] = &ValueVersion{
			Value:   key,
			Version: 0,
		}
	}
}

func (s *OccStorage) PrintStatus(op *PrintStatusRequestOp) {
	s.waitPrintStatusRequest = append(s.waitPrintStatusRequest, op)
	s.totalCommit += op.committedTxn
	s.print()
}

func (s *OccStorage) print() {
	if len(s.waitPrintStatusRequest) == s.server.config.GetTotalClient() && s.totalCommit == s.committed {
		printCommitOrder(s.txnStore, s.committed, s.server.serverId)
		printModifiedData(s.kvStore, s.server.serverId)
		for _, printOp := range s.waitPrintStatusRequest {
			printOp.wait <- true
		}
		s.checkWaiting()
	}
}

func (s *OccStorage) checkWaiting() {
	for key, n := range s.preparedReadKey {
		if n != 0 {
			log.Fatalf("key %v should have %v txn read prepared", key, n)
		}
	}

	for key, n := range s.preparedWriteKey {
		if n != 0 {
			log.Fatalf("key %v should have %v txn write prepared", key, n)
		}
	}
}
