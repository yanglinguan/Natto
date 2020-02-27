package server

import (
	"Carousel-GTS/rpc"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
)

type OccStorage struct {
	kvStore  map[string]*ValueVersion
	txnStore map[string]*TxnInfo

	preparedReadKey  map[string]int
	preparedWriteKey map[string]int

	server *Server

	committed int

	waitPrintStatusRequest *PrintStatusRequestOp
}

func NewOccStorage(server *Server) *OccStorage {
	o := &OccStorage{
		kvStore:          make(map[string]*ValueVersion),
		txnStore:         make(map[string]*TxnInfo),
		preparedReadKey:  make(map[string]int),
		preparedWriteKey: make(map[string]int),
		server:           server,
		committed:        0,
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
	if s.waitPrintStatusRequest != nil && s.waitPrintStatusRequest.committedTxn == s.committed {
		s.PrintCommitOrder()
		s.PrintModifiedData()
		s.waitPrintStatusRequest.wait <- true
	}
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
			log.Infof("ABORT %v (coordinator)", txnId)
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
			Value:            key,
			Version:          0,
			WaitingOp:        nil,
			PreparedTxnRead:  nil,
			PreparedTxnWrite: nil,
		}
	}
}

func (s *OccStorage) PrintCommitOrder() {
	txnId := make([]string, len(s.txnStore))
	for id, info := range s.txnStore {
		txnId[info.commitOrder] = id
	}

	file, err := os.Create(s.server.serverId + "_commitOrder.log")
	if err != nil || file == nil {
		log.Fatal("Fails to create log file: statistic.log")
		return
	}

	for _, id := range txnId {
		_, err = file.WriteString(id + "\n")
		if err != nil {
			log.Fatalf("Cannot write to file %v", err)
		}
	}

	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close file %v", err)
	}
}

func (s *OccStorage) PrintModifiedData() {
	file, err := os.Create(s.server.serverId + "_db.log")
	if err != nil || file == nil {
		log.Fatal("Fails to create log file: statistic.log")
		return
	}

	_, err = file.WriteString("#key, value, version\n")
	if err != nil {
		log.Fatalf("Cannot write to file, %v", err)
		return
	}

	for key, kv := range s.kvStore {
		if kv.Version == 0 {
			continue
		}

		var k int
		_, err := fmt.Sscan(key, &k)
		if err != nil {
			log.Fatalf("key %v is invalid", key)
		}

		var v int
		_, err = fmt.Sscan(kv.Value, &v)
		if err != nil {
			log.Fatalf("value %v is invalid", kv.Value)
		}

		s := fmt.Sprintf("%v,%v,%v\n",
			k,
			v,
			kv.Version)
		_, err = file.WriteString(s)
		if err != nil {
			log.Fatalf("fail to write %v", err)
		}
	}

	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close file %v", err)
	}
}

func (s *OccStorage) PrintStatus(op *PrintStatusRequestOp) {
	if s.committed < op.committedTxn {
		s.waitPrintStatusRequest = op
		return
	}
	s.PrintCommitOrder()
	s.PrintModifiedData()
	op.wait <- true
}
