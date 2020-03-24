package server

import (
	"Carousel-GTS/rpc"
	"bytes"
	"container/list"
	"encoding/gob"
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

type ReplicationMsgType int32

// Message type
const (
	PrepareResultMsg ReplicationMsgType = iota
	CommitResultMsg
	//LeaderAbortMsg        = "4"
	//CoordinatorAbortMsg   = "5"
)

type ReplicationMsg struct {
	TxnId             string
	Status            TxnStatus
	MsgType           ReplicationMsgType
	WriteData         []*rpc.KeyValue
	IsFromCoordinator bool
	TotalCommit       int
}

type TxnInfo struct {
	readAndPrepareRequestOp *ReadAndPrepareOp
	prepareResultOp         *PrepareResultOp
	status                  TxnStatus
	receiveFromCoordinator  bool
	sendToClient            bool
	sendToCoordinator       bool
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
	ApplyReplicationMsg(msg ReplicationMsg)
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
	waitPrintStatusRequest *PrintStatusRequestOp
	totalCommit            int
}

func NewAbstractStorage(server *Server) *AbstractStorage {
	s := &AbstractStorage{
		kvStore:     make(map[string]*KeyInfo),
		server:      server,
		txnStore:    make(map[string]*TxnInfo),
		committed:   0,
		totalCommit: 0,
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
	s.waitPrintStatusRequest = op
	s.totalCommit = op.committedTxn
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
	log.Debugf("total commit %v committed %v", s.totalCommit, s.committed)
	if s.waitPrintStatusRequest != nil && s.totalCommit == s.committed {
		s.printCommitOrder()
		s.printModifiedData()
		s.checkWaiting()
		s.waitPrintStatusRequest.wait <- true
	}
}

func (s AbstractStorage) printCommitOrder() {
	txnId := make([]*TxnInfo, s.committed)
	for _, info := range s.txnStore {
		if info.status == COMMIT {
			txnId[info.commitOrder] = info
		}
	}
	fName := fmt.Sprintf("s%v_%v_commitOrder.log", s.server.serverId, s.server.IsLeader())
	file, err := os.Create(fName)
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
	fName := fmt.Sprintf("s%v_%v_db.log", s.server.serverId, s.server.partitionId)
	file, err := os.Create(fName)
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
	if s.txnStore[op.request.Txn.TxnId].sendToClient {
		log.Debugf("txn %v read result already sent to client", op.request.Txn.TxnId)
		return
	}
	s.txnStore[op.request.Txn.TxnId].sendToClient = true

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
func (s *AbstractStorage) setPrepareResult(op *ReadAndPrepareOp) *PrepareResultOp {
	txnId := op.request.Txn.TxnId
	if s.txnStore[txnId].prepareResultOp != nil {
		return s.txnStore[txnId].prepareResultOp
	}
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
	return &PrepareResultOp{
		Request:          op.prepareResult,
		CoordPartitionId: int(op.request.Txn.CoordPartitionId),
	}

	// ready to send the coordinator
	//s.server.executor.PrepareResult <- &PrepareResultOp{
	//	Request:          op.prepareResult,
	//	CoordPartitionId: int(op.request.Txn.CoordPartitionId),
	//}
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
			isTop := s.kvStore[key].WaitingOp.Front().Value.(*ReadAndPrepareOp).request.Txn.TxnId == txnId
			s.kvStore[key].WaitingOp.Remove(s.kvStore[key].WaitingItem[txnId])
			delete(s.kvStore[key].WaitingItem, txnId)
			if !isTop {
				continue
			}
		}
		// otherwise, check if the top of the queue can prepare
		log.Debugf("txn %v release key %v check if txn can be prepared", txnId, key)
		s.checkPrepare(key)
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
				log.Debugf("txn %v has txn in queue key %v top of queue is %v",
					op.request.Txn.TxnId, key, top.request.Txn.TxnId)
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
	log.Debugf("PREPARED txn %v", op.request.Txn.TxnId)
	s.removeFromQueue(op)
	// record the prepared keys
	txnId := op.request.Txn.TxnId
	s.txnStore[txnId].status = PREPARED
	s.recordPrepared(op)
	s.txnStore[txnId].prepareResultOp = s.setPrepareResult(op)
	//if s.server.config.GetReplication() {
	s.replicatePreparedResult(op.request.Txn.TxnId)
	//} else {
	//	s.setReadResult(op)
	//	s.readyToSendPrepareResultToCoordinator(s.txnStore[txnId].prepareResultOp)
	//}
}

func (s *AbstractStorage) convertReplicationMsgToByte(txnId string, msgType ReplicationMsgType) bytes.Buffer {
	replicationMsg := ReplicationMsg{
		TxnId:             txnId,
		Status:            s.txnStore[txnId].status,
		MsgType:           msgType,
		IsFromCoordinator: false,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replicationMsg); err != nil {
		log.Errorf("replication encoding error: %v", err)
	}
	return buf
}

func (s *AbstractStorage) replicatePreparedResult(txnId string) {
	if !s.server.config.GetReplication() {
		log.Debugf("txn %v config no replication send result to coordinator", txnId)
		s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
		s.readyToSendPrepareResultToCoordinator(s.txnStore[txnId].prepareResultOp)
		return
	}
	//Replicates the prepare result to followers.

	buf := s.convertReplicationMsgToByte(txnId, PrepareResultMsg)

	log.Debugf("txn %s replicates the prepare result %v.", txnId, s.txnStore[txnId].status)

	s.server.raft.raftInputChannel <- string(buf.Bytes())
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

func (s *AbstractStorage) writeToDB(op []*rpc.KeyValue) {
	for _, kv := range op {
		s.kvStore[kv.Key].Value = kv.Value
		s.kvStore[kv.Key].Version++
	}
}

func (s *AbstractStorage) Abort(op *AbortRequestOp) {
	if op.isFromCoordinator {
		s.coordinatorAbort(op.abortRequest)
	} else {
		txnId := op.request.request.Txn.TxnId
		s.txnStore[txnId].prepareResultOp = s.setPrepareResult(op.request)
		s.replicatePreparedResult(txnId)
	}
}

func (s *AbstractStorage) readyToSendPrepareResultToCoordinator(op *PrepareResultOp) {
	txnId := op.Request.TxnId
	if s.txnStore[txnId].receiveFromCoordinator || s.txnStore[txnId].sendToCoordinator {
		log.Debugf("txn %v already receive from coordinator %v or send to coordinator %v",
			txnId, s.txnStore[txnId].receiveFromCoordinator, s.txnStore[txnId].sendToCoordinator)
		return
	}
	s.txnStore[txnId].sendToCoordinator = true
	log.Debugf("txn %v send prepare result to coordinator %v", txnId, op.CoordPartitionId)
	s.server.executor.PrepareResult <- s.txnStore[txnId].prepareResultOp
}

func (s *AbstractStorage) replicateCommitResult(txnId string) {
	if !s.server.config.GetReplication() {
		log.Debugf("txn %v config no replication", txnId)
		return
	}
	log.Debugf("txn %v replicate commit result %v", txnId, s.txnStore[txnId].status)
	buf := s.convertReplicationMsgToByte(txnId, CommitResultMsg)
	s.server.raft.raftInputChannel <- string(buf.Bytes())
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
		s.replicateCommitResult(txnId)
	}
}

func (s *AbstractStorage) initTxnIfNotExist(txnId string) {
	if _, exist := s.txnStore[txnId]; !exist {
		s.txnStore[txnId] = &TxnInfo{
			readAndPrepareRequestOp: nil,
			prepareResultOp:         nil,
			status:                  INIT,
			receiveFromCoordinator:  false,
			sendToClient:            false,
			sendToCoordinator:       false,
			commitOrder:             0,
			waitingTxnKey:           0,
			waitingTxnDep:           0,
			startTime:               time.Time{},
			preparedTime:            time.Time{},
			commitTime:              time.Time{},
			canReorder:              0,
		}
	}
}

func (s *AbstractStorage) ApplyReplicationMsg(msg ReplicationMsg) {
	switch msg.MsgType {
	case PrepareResultMsg:
		log.Debugf("txn %v apply prepare result %v", msg.TxnId, msg.Status)
		if s.server.IsLeader() {
			s.setReadResult(s.txnStore[msg.TxnId].readAndPrepareRequestOp)
			s.readyToSendPrepareResultToCoordinator(s.txnStore[msg.TxnId].prepareResultOp)
		} else {
			s.initTxnIfNotExist(msg.TxnId)
			if s.txnStore[msg.TxnId].status != INIT {
				log.Fatalf("txn %v in follower should be INIT but it is %v", msg.TxnId, s.txnStore[msg.TxnId].status)
			}
			s.txnStore[msg.TxnId].status = msg.Status
		}
		break
	case CommitResultMsg:
		log.Debugf("txn %v apply commit result %v", msg.TxnId, msg.Status)
		if s.server.IsLeader() {
			break
		}
		s.initTxnIfNotExist(msg.TxnId)
		s.txnStore[msg.TxnId].status = msg.Status

		if msg.Status == COMMIT {
			s.txnStore[msg.TxnId].commitOrder = s.committed
			s.committed++
			s.txnStore[msg.TxnId].commitTime = time.Now()
			s.writeToDB(msg.WriteData)
			s.print()
		}

		break
	default:
		log.Fatalf("invalid msg type %v", msg.Status)
		break
	}
}
