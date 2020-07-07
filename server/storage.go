package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
	"time"
)

type Storage struct {
	kvStore                *KVStore
	server                 *Server
	txnStore               map[string]*TxnInfo
	committed              int
	waitPrintStatusRequest *PrintStatusRequest
	totalCommit            int

	operations chan Operation
}

// only this thread can modify the kv store and txn store
func (s *Storage) executeOperations() {
	for {
		op := <-s.operations
		op.Execute(s)
	}
}

func (s *Storage) AddOperation(op Operation) {
	s.operations <- op
}

func NewStorage(server *Server) *Storage {
	s := &Storage{
		kvStore:                NewKVStore(server),
		server:                 server,
		txnStore:               make(map[string]*TxnInfo),
		committed:              0,
		waitPrintStatusRequest: nil,
		totalCommit:            0,
		operations:             make(chan Operation, server.config.GetQueueLen()),
	}
	go s.executeOperations()
	return s
}

func (s *Storage) sendPrepareResult(txnId string) {
	coordinatorPartitionId := s.txnStore[txnId].readAndPrepareRequestOp.GetCoordinatorPartitionId()
	dstServerId := s.server.config.GetLeaderIdByPartitionId(coordinatorPartitionId)
	sender := NewPrepareResultSender(s.txnStore[txnId].prepareResultRequest, dstServerId, s.server)
	go sender.Send()
}

func (s *Storage) sendReverseReorderRequest(txnId string) {
	for _, txn := range s.txnStore[txnId].prepareResultRequest.Reorder {
		log.Debugf("txn %v request txn %v to reverse reorder", txnId, txn)
		request := &rpc.ReverseReorderRequest{
			TxnId:            txnId,
			ReorderedTxnId:   txn,
			PartitionId:      int32(s.server.partitionId),
			CoordPartitionId: int32(s.txnStore[txnId].readAndPrepareRequestOp.GetCoordinatorPartitionId()),
			Counter:          s.txnStore[txnId].prepareCounter,
		}
		coordinatorPartitionId := s.txnStore[txn].readAndPrepareRequestOp.GetCoordinatorPartitionId()
		dstServerId := s.server.config.GetLeaderIdByPartitionId(coordinatorPartitionId)
		sender := NewReverseReorderRequestSender(request, dstServerId, s.server)
		go sender.Send()
	}
}

func (s *Storage) checkKeysAvailable(op ReadAndPrepareOp) bool {
	txnId := op.GetTxnId()
	for _, rk := range op.GetReadKeys() {
		if s.kvStore.IsTxnHoldWrite(rk) {
			log.Debugf("txn %v (read) : there is txn holding (write) hold key %v", txnId, rk)
			return false
		}
	}

	for _, wk := range op.GetWriteKeys() {
		if s.kvStore.IsTxnHoldWrite(wk) || s.kvStore.IsTxnHoldRead(wk) {
			log.Debugf("txn %v (write) : there is txn hold key %v", txnId, wk)
			return false
		}
	}

	return true
}

func (s *Storage) LoadKeys(keys []string) {
	log.Debugf("server load key %v", len(keys))
	for _, key := range keys {
		s.kvStore.AddKeyValue(key, key)
	}
}

func (s *Storage) HasKey(key string) bool {
	return s.kvStore.ContainsKey(key)
}

func (s *Storage) setTxnStatus(txnId string, status TxnStatus) {
	if _, exist := s.txnStore[txnId]; !exist {
		log.Fatalf("txn %v dose not exist cannot set status", txnId)
	}
	s.txnStore[txnId].status = status
}

// set the read value, return back to client
func (s Storage) setReadResult(op ReadAndPrepareOp, status TxnStatus, setStatus bool) {
	txnId := op.GetTxnId()
	log.Debugf("txn %v set read result", txnId)
	if s.txnStore[txnId].sendToClient {
		log.Debugf("txn %v read result already sent to client", txnId)
		return
	}
	s.txnStore[txnId].sendToClient = true

	reply := &rpc.ReadAndPrepareReply{
		KeyValVerList: make([]*rpc.KeyValueVersion, 0),
		Status:        -1,
		IsLeader:      s.server.IsLeader(),
	}

	if setStatus {
		reply.Status = int32(status)
	}

	if reply.Status != int32(ABORT) {
		log.Debugf("get key %v", op.GetReadKeys())
		for _, rk := range op.GetReadKeys() {
			//log.Debugf("get key %v", rk)
			value, version := s.kvStore.Get(rk)
			keyValueVersion := &rpc.KeyValueVersion{
				Key:     rk,
				Value:   value,
				Version: version,
			}
			reply.KeyValVerList = append(reply.KeyValVerList, keyValueVersion)
		}
	}

	op.SetReadReply(reply)
	op.UnblockClient()
}

// set prepared or abort result
func (s *Storage) setPrepareResult(op ReadAndPrepareOp) {
	txnId := op.GetTxnId()
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
		Conditions:      make([]int32, 0),
		Counter:         s.txnStore[txnId].prepareCounter,
	}

	s.txnStore[txnId].prepareCounter++

	if s.txnStore[txnId].status != ABORT {
		s.txnStore[txnId].preparedTime = time.Now()

		for _, rk := range op.GetReadKeys() {
			_, version := s.kvStore.Get(rk)
			prepareResultRequest.ReadKeyVerList = append(prepareResultRequest.ReadKeyVerList,
				&rpc.KeyVersion{
					Key:     rk,
					Version: version,
				},
			)
		}

		for _, wk := range op.GetWriteKeys() {
			_, version := s.kvStore.Get(wk)
			prepareResultRequest.WriteKeyVerList = append(prepareResultRequest.WriteKeyVerList,
				&rpc.KeyVersion{
					Key:     wk,
					Version: version,
				},
			)
		}
	}

	s.txnStore[txnId].prepareResultRequest = prepareResultRequest
}

func (s *Storage) replicatePreparedResult(txnId string) {
	if s.server.config.GetFastPath() {
		op := NewFastPathPrepareResultReplication(txnId)
		op.Execute(s)
	}

	if s.server.IsLeader() {
		op := NewPrepareResultReplicationOp(txnId)
		op.Execute(s)
	}
}

func (s *Storage) replicateCommitResult(txnId string, writeData []*rpc.KeyValue) {
	op := NewCommitResultReplication(txnId, writeData)
	op.Execute(s)
}

func (s *Storage) selfAbort(op ReadAndPrepareOp) {
	s.txnStore[op.GetTxnId()].status = ABORT
	s.setPrepareResult(op)
	s.replicatePreparedResult(op.GetTxnId())
}

func (s *Storage) initTxnIfNotExist(msg ReplicationMsg) bool {
	if _, exist := s.txnStore[msg.TxnId]; !exist {
		s.txnStore[msg.TxnId] = NewTxnInfo()
		if msg.Status == PREPARED {
			s.txnStore[msg.TxnId].readAndPrepareRequestOp =
				s.server.operationCreator.createReadAndPrepareOpWithReplicationMsg(msg)
			s.txnStore[msg.TxnId].prepareResultRequest = &rpc.PrepareResultRequest{
				TxnId:           msg.TxnId,
				ReadKeyVerList:  msg.PreparedReadKeyVersion,
				WriteKeyVerList: msg.PreparedWriteKeyVersion,
				PartitionId:     int32(s.server.partitionId),
				PrepareStatus:   int32(msg.Status),
			}
		}
		return false
	}
	return true
}

func (s *Storage) writeToDB(kvs []*rpc.KeyValue) {
	for _, kv := range kvs {
		s.kvStore.Put(kv.Key, kv.Value)
	}
}

func (s *Storage) prepare(op ReadAndPrepareOp) {
	s.txnStore[op.GetTxnId()].status = PREPARED
	s.kvStore.RecordPrepared(op)
	s.setPrepareResult(op)
	s.replicatePreparedResult(op.GetTxnId())
}

func (s *Storage) AddTxn(op ReadAndPrepareOp) {
	txnId := op.GetTxnId()
	log.Debugf("txn %v add to txn store", txnId)
	t := NewTxnInfo()
	t.readAndPrepareRequestOp = op
	t.startTime = time.Now()
	s.txnStore[txnId] = t
}

func (s *Storage) checkAbort(op ReadAndPrepareOp) bool {
	txnId := op.GetTxnId()
	if info, exist := s.txnStore[txnId]; exist && info.status != INIT {
		log.Debugf("txn %v is already has status %v", txnId, s.txnStore[txnId].status)
		s.setReadResult(op, -1, false)
		return true
	}
	return false
}

func (s *Storage) commit(txnId string, status TxnStatus, writeData []*rpc.KeyValue) {
	s.txnStore[txnId].status = status
	if status != COMMIT {
		log.Debugf("txn %v is not commit %v", txnId, status)
		return
	}

	s.txnStore[txnId].commitOrder = s.committed
	s.committed++
	s.txnStore[txnId].commitTime = time.Now()
	s.writeToDB(writeData)
	s.print()
}
