package server

import (
	"Carousel-GTS/rpc"
	"container/list"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

type KeyType int

const (
	READ KeyType = iota
	WRITE
)

type TxnStatus int32

const (
	INIT TxnStatus = iota
	PREPARED
	COMMIT
	ABORT
)

type TxnInfo struct {
	readAndPrepareRequestOp *ReadAndPrepareOp
	status                  TxnStatus
	receiveFromCoordinator  bool
	commitOrder             int
	waitingTxnKey           int
	waitingTxnDep           int
	startTime               time.Time
	preparedTime            time.Time
	commitTime              time.Time
	canReorder              int
}

type KeyInfo struct {
	Value            string
	Version          uint64
	WaitingOp        *list.List
	WaitingItem      map[string]*list.Element
	PreparedTxnRead  map[string]bool
	PreparedTxnWrite map[string]bool
}

type Storage interface {
	Prepare(op *ReadAndPrepareOp)
	Commit(op *CommitRequestOp)
	Abort(op *AbortRequestOp)
	LoadKeys(keys []string)
	PrintStatus(op *PrintStatusRequestOp)
	HasKey(key string) bool
}

type AbstractMethod interface {
	checkKeysAvailable(op *ReadAndPrepareOp) bool
	prepared(op *ReadAndPrepareOp)
	abortProcessedTxn(txnId string)
}

type AbstractStorage struct {
	Storage

	abstractMethod AbstractMethod

	kvStore                map[string]*KeyInfo
	server                 *Server
	txnStore               map[string]*TxnInfo
	committed              int
	waitPrintStatusRequest []*PrintStatusRequestOp
	totalCommit            int
}

func NewAbstractStorage(server *Server) *AbstractStorage {
	s := &AbstractStorage{
		kvStore:                make(map[string]*KeyInfo),
		server:                 server,
		txnStore:               make(map[string]*TxnInfo),
		committed:              0,
		waitPrintStatusRequest: make([]*PrintStatusRequestOp, 0),
		totalCommit:            0,
	}

	return s
}

func (s *AbstractStorage) LoadKeys(keys []string) {
	for _, key := range keys {
		s.kvStore[key] = &KeyInfo{
			Value:            key,
			Version:          0,
			WaitingOp:        list.New(),
			WaitingItem:      make(map[string]*list.Element),
			PreparedTxnRead:  make(map[string]bool),
			PreparedTxnWrite: make(map[string]bool),
		}
	}
}

func (s *AbstractStorage) HasKey(key string) bool {
	_, exist := s.kvStore[key]
	return exist
}

func (s *AbstractStorage) PrintStatus(op *PrintStatusRequestOp) {
	s.waitPrintStatusRequest = append(s.waitPrintStatusRequest, op)
	s.totalCommit += op.committedTxn
	s.print()
}

func (s *AbstractStorage) checkWaiting() {
	for key, kv := range s.kvStore {
		if len(kv.PreparedTxnRead) != 0 ||
			len(kv.PreparedTxnWrite) != 0 ||
			kv.WaitingOp.Len() != 0 ||
			len(kv.WaitingItem) != 0 {
			for rt := range kv.PreparedTxnRead {
				log.Errorf("txn %v prepared for read key %v", rt, key)
			}
			for wt := range kv.PreparedTxnWrite {
				log.Errorf("txn %v prepared for write key %v", wt, key)
			}
			for txn := range kv.WaitingItem {
				log.Errorf("txn %v is wait for key %v", txn, key)
			}
			log.Fatalf("key %v should have waiting txn", key)
		}
	}
}

func (s *AbstractStorage) print() {
	if len(s.waitPrintStatusRequest) == s.server.config.GetTotalClient() && s.totalCommit == s.committed {
		s.printCommitOrder()
		s.printModifiedData()
		s.checkWaiting()
		for _, printOp := range s.waitPrintStatusRequest {
			printOp.wait <- true
		}
	}
}

func (s AbstractStorage) printCommitOrder() {
	txnId := make([]*TxnInfo, s.committed)
	for _, info := range s.txnStore {
		if info.status == COMMIT {
			txnId[info.commitOrder] = info
		}
	}

	file, err := os.Create(s.server.serverId + "_commitOrder.log")
	if err != nil || file == nil {
		log.Fatal("Fails to create log file: statistic.log")
		return
	}

	for _, info := range txnId {
		s := fmt.Sprintf("%v %v %v %v %v %v %v\n",
			info.readAndPrepareRequestOp.request.Txn.TxnId,
			info.waitingTxnKey,
			info.waitingTxnDep,
			info.preparedTime.Sub(info.startTime).Nanoseconds(),
			info.commitTime.Sub(info.preparedTime).Nanoseconds(),
			info.commitTime.Sub(info.startTime).Nanoseconds(),
			info.canReorder)
		_, err = file.WriteString(s)
		if err != nil {
			log.Fatalf("Cannot write to file %v", err)
		}
	}

	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close file %v", err)
	}
}

func (s AbstractStorage) printModifiedData() {
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

// set the read value, return back to client
func (s AbstractStorage) setReadResult(op *ReadAndPrepareOp) {
	op.reply = &rpc.ReadAndPrepareReply{
		KeyValVerList: make([]*rpc.KeyValueVersion, 0),
		IsAbort:       s.txnStore[op.request.Txn.TxnId].status == ABORT,
	}

	if !op.reply.IsAbort {
		for rk := range op.readKeyMap {
			keyValueVersion := &rpc.KeyValueVersion{
				Key:     rk,
				Value:   s.kvStore[rk].Value,
				Version: s.kvStore[rk].Version,
			}
			op.reply.KeyValVerList = append(op.reply.KeyValVerList, keyValueVersion)
		}
	}

	op.wait <- true
}

// set prepared or abort result
func (s *AbstractStorage) setPrepareResult(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	if _, exist := s.txnStore[txnId]; !exist {
		log.Fatalln("txn %v txnInfo should be created, and INIT status", txnId)
	}

	op.prepareResult = &rpc.PrepareResultRequest{
		TxnId:           txnId,
		ReadKeyVerList:  make([]*rpc.KeyVersion, 0),
		WriteKeyVerList: make([]*rpc.KeyVersion, 0),
		PartitionId:     int32(s.server.partitionId),
		PrepareStatus:   int32(s.txnStore[op.request.Txn.TxnId].status),
	}

	if s.txnStore[op.request.Txn.TxnId].status == PREPARED {
		s.txnStore[op.request.Txn.TxnId].preparedTime = time.Now()

		for rk := range op.readKeyMap {
			op.prepareResult.ReadKeyVerList = append(op.prepareResult.ReadKeyVerList,
				&rpc.KeyVersion{
					Key:     rk,
					Version: s.kvStore[rk].Version,
				},
			)
		}

		for wk := range op.writeKeyMap {
			op.prepareResult.WriteKeyVerList = append(op.prepareResult.WriteKeyVerList,
				&rpc.KeyVersion{
					Key:     wk,
					Version: s.kvStore[wk].Version,
				},
			)
		}

		op.sendToCoordinator = true
	}

	// ready to send the coordinator
	s.server.executor.PrepareResult <- &PrepareResultOp{
		Request:          op.prepareResult,
		CoordPartitionId: int(op.request.Txn.CoordPartitionId),
	}
}

// add txn to the queue waiting for keys
func (s *AbstractStorage) addToQueue(keys map[string]bool, op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	log.Infof("Txn %v wait for keys", txnId)
	for key := range keys {
		log.Debugf("Txn %v wait for key %v", txnId, key)
		item := s.kvStore[key].WaitingOp.PushBack(op)
		s.kvStore[key].WaitingItem[txnId] = item
	}
}

// release the keys that txn holds
// check if there is txn can be prepared when keys are released
func (s *AbstractStorage) release(txnId string) {
	txnInfo := s.txnStore[txnId]
	// remove prepared read and write key
	for rk, isPrepared := range txnInfo.readAndPrepareRequestOp.readKeyMap {
		if isPrepared {
			log.Debugf("txn %v release read key %v", txnId, rk)
			delete(s.kvStore[rk].PreparedTxnRead, txnId)
		}
	}

	for wk, isPrepared := range txnInfo.readAndPrepareRequestOp.writeKeyMap {
		if isPrepared {
			log.Debugf("txn %v release write key %v", txnId, wk)
			delete(s.kvStore[wk].PreparedTxnWrite, txnId)
		}
	}

	for key := range txnInfo.readAndPrepareRequestOp.keyMap {
		if _, exist := s.kvStore[key].WaitingItem[txnId]; exist {
			// if in the queue, then remove from the queue
			log.Debugf("remove txn %v from key %v queue", txnId, key)
			s.kvStore[key].WaitingOp.Remove(s.kvStore[key].WaitingItem[txnId])
			delete(s.kvStore[key].WaitingItem, txnId)
		} else {
			// otherwise, check if the top of the queue can prepare
			log.Debugf("txn %v release key %v check if txn can be prepared", txnId, key)
			s.checkPrepare(key)
		}
	}
}

// check if all keys are available
func (s *AbstractStorage) checkKeysAvailable(op *ReadAndPrepareOp) bool {
	available := true
	for rk := range op.readKeyMap {
		if len(s.kvStore[rk].PreparedTxnWrite) > 0 {
			log.Debugf("txn %v cannot prepare because of cannot get read key %v: %v", op.request.Txn.TxnId, rk, s.kvStore[rk].PreparedTxnWrite)
			available = false
			break
		}
	}

	for wk := range op.writeKeyMap {
		if !available ||
			len(s.kvStore[wk].PreparedTxnRead) > 0 ||
			len(s.kvStore[wk].PreparedTxnWrite) > 0 {
			log.Debugf("txn %v cannot prepare because of cannot get write key %v: read %v write %v",
				op.request.Txn.TxnId, wk, s.kvStore[wk].PreparedTxnRead, s.kvStore[wk].PreparedTxnWrite)
			available = false
			break
		}
	}

	return available
}

// check if there is txn waiting for keys
func (s *AbstractStorage) hasWaitingTxn(op *ReadAndPrepareOp) bool {
	for key := range op.keyMap {
		if s.kvStore[key].WaitingOp.Len() > 0 {
			top := s.kvStore[key].WaitingOp.Front().Value.(*ReadAndPrepareOp)
			if top.request.Txn.TxnId != op.request.Txn.TxnId {
				return true
			}
		}
	}

	return false
}

func (s *AbstractStorage) recordPrepared(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	for rk := range op.readKeyMap {
		op.RecordPreparedKey(rk, READ)
		s.kvStore[rk].PreparedTxnRead[txnId] = true
	}
	for wk := range op.writeKeyMap {
		op.RecordPreparedKey(wk, WRITE)
		s.kvStore[wk].PreparedTxnWrite[txnId] = true
	}
}

func (s *AbstractStorage) removeFromQueue(op *ReadAndPrepareOp) {
	// remove from the top of the queue
	txnId := op.request.Txn.TxnId
	for key := range op.keyMap {
		if _, exist := s.kvStore[key].WaitingItem[txnId]; !exist {
			continue
		}
		if s.kvStore[key].WaitingOp.Front().Value.(*ReadAndPrepareOp).request.Txn.TxnId != txnId {
			log.Fatalf("txn %v is not front of queue key %v", txnId, key)
			return
		}

		s.kvStore[key].WaitingOp.Remove(s.kvStore[key].WaitingItem[txnId])
		delete(s.kvStore[key].WaitingItem, txnId)
	}
}

func (s *AbstractStorage) prepared(op *ReadAndPrepareOp) {
	s.removeFromQueue(op)
	// record the prepared keys
	s.txnStore[op.request.Txn.TxnId].status = PREPARED
	s.recordPrepared(op)
	s.setReadResult(op)
	s.setPrepareResult(op)
}

// check if there is txn can be prepared when key is released
func (s *AbstractStorage) checkPrepare(key string) {
	for s.kvStore[key].WaitingOp.Len() != 0 {
		e := s.kvStore[key].WaitingOp.Front()
		op := e.Value.(*ReadAndPrepareOp)
		txnId := op.request.Txn.TxnId
		// skip the aborted txn
		if txnInfo, exist := s.txnStore[txnId]; exist && txnInfo.status == ABORT {
			log.Debugf("txn %v is already abort remove from the queue of key %v", txnId, key)
			s.kvStore[key].WaitingOp.Remove(e)
			continue
		}

		// check if the txn can acquire all the keys
		canPrepare := s.abstractMethod.checkKeysAvailable(op)
		hasWaiting := s.hasWaitingTxn(op)
		if !canPrepare || hasWaiting {
			log.Infof("cannot prepare %v had waiting %v, can prepare %v when release key %v",
				op.request.Txn.TxnId, hasWaiting, canPrepare, key)
			break
		}

		log.Infof("can prepare %v when key %v is released", op.request.Txn.TxnId, key)
		s.abstractMethod.prepared(op)
	}
}

func (s *AbstractStorage) writeToDB(op *CommitRequestOp) {
	for _, kv := range op.request.WriteKeyValList {
		s.kvStore[kv.Key].Value = kv.Value
		s.kvStore[kv.Key].Version++
	}
}

func (s *AbstractStorage) Abort(op *AbortRequestOp) {
	if op.isFromCoordinator {
		s.coordinatorAbort(op.abortRequest)
	} else {
		op.sendToCoordinator = !s.txnStore[op.request.request.Txn.TxnId].receiveFromCoordinator
		if op.sendToCoordinator {
			s.setPrepareResult(op.request)
		}
	}
}

func (s *AbstractStorage) selfAbort(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	log.Debugf("txn %v passed timestamp also cannot prepared", txnId)
	abortOp := NewAbortRequestOp(nil, op, false)
	s.server.executor.AbortTxn <- abortOp
}

func (s *AbstractStorage) coordinatorAbort(request *rpc.AbortRequest) {
	txnId := request.TxnId
	if txnInfo, exist := s.txnStore[txnId]; exist {
		txnInfo.receiveFromCoordinator = true
		switch txnInfo.status {
		case ABORT:
			log.Infof("txn %v is already abort it self", txnId)
			break
		case COMMIT:
			log.Fatalf("Error: txn %v is already committed", txnId)
			break
		default:
			log.Debugf("call abort processed txn %v", txnId)
			s.abstractMethod.abortProcessedTxn(txnId)
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
