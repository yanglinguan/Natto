package server

import (
	"Carousel-GTS/rpc"
	"container/list"
	log "github.com/sirupsen/logrus"
)

type GTSStorageDepGraph struct {
	kvStore  map[string]*ValueVersion
	server   *Server
	txnStore map[string]*TxnInfo

	graph *Graph

	// txnId set
	readyToCommitTxn map[string]bool
	// txnId -> commitRequestOp
	waitToCommitTxn map[string]*CommitRequestOp
}

func NewGTSStorageDepGraph(server *Server) *GTSStorageDepGraph {
	s := &GTSStorageDepGraph{
		kvStore:          make(map[string]*ValueVersion),
		server:           server,
		txnStore:         make(map[string]*TxnInfo),
		graph:            NewDependencyGraph(),
		readyToCommitTxn: make(map[string]bool),
		waitToCommitTxn:  make(map[string]*CommitRequestOp),
	}

	return s
}

func (s *GTSStorageDepGraph) getNextCommitListByCommitOrAbort(txnId string) {
	s.graph.Remove(txnId)
	for _, txn := range s.graph.GetNext() {
		s.readyToCommitTxn[txn] = true
		if _, exist := s.waitToCommitTxn[txn]; exist {
			s.waitToCommitTxn[txn].canCommit = true
			s.server.executor.CommitTxn <- s.waitToCommitTxn[txn]
			delete(s.waitToCommitTxn, txn)
		}
	}
}

func (s *GTSStorageDepGraph) checkCommit(txnId string) bool {
	if len(s.readyToCommitTxn) == 0 {
		for _, txn := range s.graph.GetNext() {
			s.readyToCommitTxn[txn] = true
		}
	}

	if _, exist := s.readyToCommitTxn[txnId]; exist {
		return true
	} else {
		return false
	}
}

func (s *GTSStorageDepGraph) Commit(op *CommitRequestOp) {

	txnId := op.request.TxnId
	if txnInfo, exist := s.txnStore[txnId]; !exist || txnInfo.status != PREPARED {
		log.WithFields(log.Fields{
			"txnId":  txnId,
			"status": txnInfo.status,
		}).Fatal("txn should be prepared before commit")
	}

	if op.canCommit || s.checkCommit(txnId) {
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

		s.getNextCommitListByCommitOrAbort(txnId)

		delete(s.readyToCommitTxn, txnId)
		op.wait <- true
	} else {
		s.waitToCommitTxn[txnId] = op
	}
}

func (s *GTSStorageDepGraph) selfAbort(op *ReadAndPrepareOp) {
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
		log.WithFields(log.Fields{
			"txnId": txnId,
		}).Debugln("receive readAndPrepareRequest, abort txn, create txnInfo")

		s.txnStore[txnId] = &TxnInfo{
			readAndPrepareRequestOp: op,
			status:                  ABORT,
			receiveFromCoordinator:  false,
		}
	}

}

func (s *GTSStorageDepGraph) coordinatorAbort(request *rpc.AbortRequest) {
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

			s.getNextCommitListByCommitOrAbort(txnId)

			for rk, isPrepared := range txnInfo.readAndPrepareRequestOp.readKeyMap {
				if isPrepared {
					delete(s.kvStore[rk].PreparedTxnRead, txnId)
				}
			}

			for wk, isPrepared := range txnInfo.readAndPrepareRequestOp.writeKeyMap {
				if isPrepared {
					delete(s.kvStore[wk].PreparedTxnWrite, txnId)
					s.checkPrepare(wk)
				}
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

func (s *GTSStorageDepGraph) Abort(op *AbortRequestOp) {
	if op.isFromCoordinator {
		s.coordinatorAbort(op.abortRequest)
	} else {
		s.selfAbort(op.request)
		op.sendToCoordinator = !s.txnStore[op.request.request.Txn.TxnId].receiveFromCoordinator
		if op.sendToCoordinator {
			s.setPrepareResult(op.request, ABORT)
		}
	}
}

// when the PreparedTxnWrite == 0, check if the waiting txn can be prepared
func (s *GTSStorageDepGraph) checkPrepare(key string) {

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
			if len(s.kvStore[key].PreparedTxnWrite) == 0 {
				op.RecordPreparedKey(key, READ)
				s.kvStore[key].PreparedTxnRead[txnId] = true
			} else {
				canPrepareThisKey = false
			}
		}

		if isPrepared, exist := op.writeKeyMap[key]; exist && !isPrepared {
			op.RecordPreparedKey(key, WRITE)
			s.kvStore[key].PreparedTxnWrite[txnId] = true
		}

		if !canPrepareThisKey {
			break
		}

		s.kvStore[key].WaitingOp.Remove(e)

		if op.IsPrepared() {
			s.addToGraph(op)
			s.setPrepareResult(op, PREPARED)
		}

	}
}

func (s *GTSStorageDepGraph) isAborted(txnId string) bool {
	if txnInfo, exist := s.txnStore[txnId]; exist && txnInfo.status == ABORT {
		return true
	}
	return false
}

func (s *GTSStorageDepGraph) prepareResult(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	if txnInfo, exist := s.txnStore[txnId]; !exist {
		log.WithFields(log.Fields{
			"txnId":   txnId,
			"txnInfo": txnInfo != nil,
		}).Fatalln("txnInfo should be created, and INIT status")
	}

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
	op.wait <- true
	// ready to send the coordinator
	s.server.executor.PrepareResult <- &PrepareResultOp{
		Request:          op.prepareResult,
		CoordPartitionId: int(op.request.Txn.CoordPartitionId),
	}
}

func (s *GTSStorageDepGraph) setPrepareResult(op *ReadAndPrepareOp, status TxnStatus) {

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

func (s *GTSStorageDepGraph) Prepare(op *ReadAndPrepareOp) {
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
		status:                  INIT,
		receiveFromCoordinator:  false,
	}

	notPreparedKey := make(map[string]bool)
	for _, rk := range op.request.Txn.ReadKeyList {
		if len(s.kvStore[rk].PreparedTxnWrite) == 0 && s.kvStore[rk].WaitingOp.Len() == 0 {
			op.RecordPreparedKey(rk, READ)
			s.kvStore[rk].PreparedTxnRead[txnId] = true
		} else {
			notPreparedKey[rk] = true
		}
	}

	for _, wk := range op.request.Txn.WriteKeyList {
		if s.kvStore[wk].WaitingOp.Len() == 0 {
			op.RecordPreparedKey(wk, WRITE)
			s.kvStore[wk].PreparedTxnWrite[txnId] = true
		} else {
			notPreparedKey[wk] = true
		}
	}

	if !op.IsPrepared() {
		for key := range notPreparedKey {
			s.kvStore[key].WaitingOp.PushBack(op)
		}
		return
	}
	s.addToGraph(op)
	s.setPrepareResult(op, PREPARED)
}

func (s *GTSStorageDepGraph) addToGraph(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	// only add to dependent graph when txn can be prepared
	if !op.IsPrepared() {
		log.WithFields(log.Fields{
			"txnId":      txnId,
			"isPrepared": false,
		}).Fatalln("cannot add a txn that is not prepared into dependency graph")
	}

	s.graph.AddNode(txnId)

	for _, wk := range op.request.Txn.WriteKeyList {
		for txn := range s.kvStore[wk].PreparedTxnRead {
			if txn != txnId {
				s.graph.AddEdge(txn, txnId)
			}
		}

		for txn := range s.kvStore[wk].PreparedTxnWrite {
			if txn != txnId {
				s.graph.AddEdge(txn, txnId)
			}
		}
	}
}

func (s *GTSStorageDepGraph) LoadKeys(keys []string) {
	for _, key := range keys {
		s.kvStore[key] = &ValueVersion{
			Value:            key,
			Version:          0,
			WaitingOp:        list.New(),
			PreparedTxnRead:  make(map[string]bool),
			PreparedTxnWrite: make(map[string]bool),
		}
	}
}
