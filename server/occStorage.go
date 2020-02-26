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
}

func NewOccStorage(server *Server) *OccStorage {
	o := &OccStorage{
		kvStore:          make(map[string]*ValueVersion),
		txnStore:         make(map[string]*TxnInfo),
		preparedReadKey:  make(map[string]int),
		preparedWriteKey: make(map[string]int),
		server:           server,
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
		op,
		INIT,
		false,
	}

	canPrepare := true
	for _, key := range op.request.Txn.ReadKeyList {
		// read write conflict
		if s.preparedWriteKey[key] > 0 {
			canPrepare = false
			break
		}
	}

	for _, key := range op.request.Txn.WriteKeyList {
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

	abortOp := &AbortRequestOp{
		abortRequest:      nil,
		request:           op,
		isFromCoordinator: false,
		sendToCoordinator: false,
	}
	s.server.executor.AbortTxn <- abortOp

}

func (s *OccStorage) Commit(op *CommitRequestOp) {
	log.Infof("commit txn %v", op.request.TxnId)
	for _, kv := range op.request.WriteKeyValList {
		s.kvStore[kv.Key].Value = kv.Value
		s.kvStore[kv.Key].Version++
		s.preparedWriteKey[kv.Key]--
	}

	for _, kv := range op.request.ReadKeyVerList {
		s.preparedReadKey[kv.Key]--
	}
	s.txnStore[op.request.TxnId].status = COMMIT
	s.txnStore[op.request.TxnId].receiveFromCoordinator = true
	op.wait <- true
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
		default:
			log.WithFields(log.Fields{
				"txnId":  txnId,
				"status": txnInfo.status,
			}).Debugln("release lock")
			txnInfo.receiveFromCoordinator = true
			txnInfo.status = ABORT
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
			Value:            key,
			Version:          0,
			WaitingOp:        nil,
			PreparedTxnRead:  nil,
			PreparedTxnWrite: nil,
		}
	}
}
