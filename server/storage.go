package server

import (
	"Carousel-GTS/rpc"
	"Carousel-GTS/utils"
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
	WAITING
	PREPARED
	COMMIT
	ABORT
)

type ReplicationMsgType int32

// Message type
const (
	PrepareResultMsg ReplicationMsgType = iota
	CommitResultMsg
	WriteDataMsg
)

type ReplicationMsg struct {
	TxnId                   string
	Status                  TxnStatus
	MsgType                 ReplicationMsgType
	WriteData               []*rpc.KeyValue
	PreparedReadKeyVersion  []*rpc.KeyVersion
	PreparedWriteKeyVersion []*rpc.KeyVersion
	IsFastPathSuccess       bool
	IsFromCoordinator       bool
	TotalCommit             int
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
	isFastPrepare           bool
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
	applyReplicatedPrepareResult(msg ReplicationMsg)
	applyReplicatedCommitResult(msg ReplicationMsg)
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
		s.checkWaiting()
		s.printCommitOrder()
		s.printModifiedData()
		s.waitPrintStatusRequest.wait <- true
	}
}

func (s AbstractStorage) printCommitOrder() {
	txnInfo := make([]*TxnInfo, s.committed)
	txnId := make([]string, s.committed)
	for id, info := range s.txnStore {
		if info.status == COMMIT {
			txnInfo[info.commitOrder] = info
			txnId[info.commitOrder] = id
		}
	}
	fName := fmt.Sprintf("s%v_%v_commitOrder.log", s.server.serverId, s.server.IsLeader())
	file, err := os.Create(fName)
	if err != nil || file == nil {
		log.Fatal("Fails to create log file: statistic.log")
		return
	}

	for i, info := range txnInfo {
		//s := fmt.Sprintf("%v %v %v %v %v\n",
		s := fmt.Sprintf("%v %v %v %v %v %v %v %v\n",
			txnId[i],
			info.waitingTxnKey,
			info.waitingTxnDep,
			info.preparedTime.Sub(info.startTime).Nanoseconds(),
			info.commitTime.Sub(info.preparedTime).Nanoseconds(),
			info.commitTime.Sub(info.startTime).Nanoseconds(),
			info.canReorder,
			info.isFastPrepare)
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

		k := utils.ConvertToInt(key)
		if err != nil {
			log.Fatalf("key %v is invalid", key)
		}

		v := utils.ConvertToInt(kv.Value)
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
	if s.txnStore[op.txnId].sendToClient {
		log.Debugf("txn %v read result already sent to client", op.txnId)
		return
	}
	s.txnStore[op.txnId].sendToClient = true

	op.reply = &rpc.ReadAndPrepareReply{
		KeyValVerList: make([]*rpc.KeyValueVersion, 0),
		IsAbort:       s.txnStore[op.txnId].status == ABORT,
	}

	if !op.reply.IsAbort && s.server.IsLeader() {
		for rk := range op.readKeyMap {
			keyValueVersion := &rpc.KeyValueVersion{
				Key:     rk,
				Value:   s.kvStore[rk].Value,
				Version: s.kvStore[rk].Version,
			}
			op.reply.KeyValVerList = append(op.reply.KeyValVerList, keyValueVersion)
		}
	}

	if !s.server.IsLeader() {
		op.reply.IsAbort = false
	}

	op.wait <- true
}

// set prepared or abort result
func (s *AbstractStorage) setPrepareResult(op *ReadAndPrepareOp) {
	txnId := op.txnId
	if _, exist := s.txnStore[txnId]; !exist {
		log.Fatalln("txn %v txnInfo should be created, and INIT status", txnId)
	}

	if s.txnStore[txnId].prepareResultOp != nil {
		return
	}

	prepareResult := &rpc.PrepareResultRequest{
		TxnId:           txnId,
		ReadKeyVerList:  make([]*rpc.KeyVersion, 0),
		WriteKeyVerList: make([]*rpc.KeyVersion, 0),
		PartitionId:     int32(s.server.partitionId),
		PrepareStatus:   int32(s.txnStore[txnId].status),
	}

	if s.txnStore[txnId].status == PREPARED {
		s.txnStore[txnId].preparedTime = time.Now()

		for rk := range op.readKeyMap {
			prepareResult.ReadKeyVerList = append(prepareResult.ReadKeyVerList,
				&rpc.KeyVersion{
					Key:     rk,
					Version: s.kvStore[rk].Version,
				},
			)
		}

		for wk := range op.writeKeyMap {
			prepareResult.WriteKeyVerList = append(prepareResult.WriteKeyVerList,
				&rpc.KeyVersion{
					Key:     wk,
					Version: s.kvStore[wk].Version,
				},
			)
		}

		op.sendToCoordinator = true
	}

	s.txnStore[txnId].prepareResultOp = NewPrepareRequestOp(prepareResult, int(op.request.Txn.CoordPartitionId))
}

// add txn to the queue waiting for keys
func (s *AbstractStorage) addToQueue(keys map[string]bool, op *ReadAndPrepareOp) {
	txnId := op.txnId
	log.Infof("Txn %v wait for keys", txnId)
	for key := range keys {
		log.Debugf("Txn %v wait for key %v", txnId, key)
		item := s.kvStore[key].WaitingOp.PushBack(op)
		s.kvStore[key].WaitingItem[txnId] = item
	}
}

func (s *AbstractStorage) releaseKey(txnId string) {
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
}

// release the keys that txn holds
// check if there is txn can be prepared when keys are released
func (s *AbstractStorage) releaseKeyAndCheckPrepare(txnId string) {
	s.releaseKey(txnId)
	for key := range s.txnStore[txnId].readAndPrepareRequestOp.keyMap {
		if _, exist := s.kvStore[key].WaitingItem[txnId]; exist {
			// if in the queue, then remove from the queue
			log.Debugf("remove txn %v from key %v queue", txnId, key)
			isTop := s.kvStore[key].WaitingOp.Front().Value.(*ReadAndPrepareOp).txnId == txnId
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
			log.Debugf("txn %v cannot prepare because of cannot get read key %v: %v", op.txnId, rk, s.kvStore[rk].PreparedTxnWrite)
			available = false
			break
		}
	}

	for wk := range op.writeKeyMap {
		if !available ||
			len(s.kvStore[wk].PreparedTxnRead) > 0 ||
			len(s.kvStore[wk].PreparedTxnWrite) > 0 {
			log.Debugf("txn %v cannot prepare because of cannot get write key %v: read %v write %v",
				op.txnId, wk, s.kvStore[wk].PreparedTxnRead, s.kvStore[wk].PreparedTxnWrite)
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
			if top.txnId != op.txnId {
				log.Debugf("txn %v has txn in queue key %v top of queue is %v",
					op.txnId, key, top.txnId)
				return true
			}
		}
	}

	return false
}

func (s *AbstractStorage) recordPrepared(op *ReadAndPrepareOp) {
	txnId := op.txnId
	for rk := range op.readKeyMap {
		op.readKeyMap[rk] = true
		s.kvStore[rk].PreparedTxnRead[txnId] = true
	}
	for wk := range op.writeKeyMap {
		op.writeKeyMap[wk] = true
		s.kvStore[wk].PreparedTxnWrite[txnId] = true
	}
}

func (s *AbstractStorage) removeFromQueue(op *ReadAndPrepareOp) {
	// remove from the top of the queue
	txnId := op.txnId
	for key := range op.keyMap {
		if _, exist := s.kvStore[key].WaitingItem[txnId]; !exist {
			continue
		}
		//if s.kvStore[key].WaitingOp.Front().Value.(*ReadAndPrepareOp).txnId != txnId {
		//	log.Fatalf("txn %v is not front of queue key %v", txnId, key)
		//	return
		//}

		s.kvStore[key].WaitingOp.Remove(s.kvStore[key].WaitingItem[txnId])
		delete(s.kvStore[key].WaitingItem, txnId)
	}
}

func (s *AbstractStorage) prepared(op *ReadAndPrepareOp) {
	log.Debugf("PREPARED txn %v", op.txnId)
	s.removeFromQueue(op)
	// record the prepared keys
	txnId := op.txnId
	s.txnStore[txnId].status = PREPARED
	s.recordPrepared(op)
	s.setReadResult(op)
	s.setPrepareResult(op)
	s.replicatePreparedResult(op.txnId)
}

func (s *AbstractStorage) replicatePreparedResult(txnId string) {
	if !s.server.config.GetReplication() {
		log.Debugf("txn %v config no replication send result to coordinator", txnId)
		s.setReadResult(s.txnStore[txnId].readAndPrepareRequestOp)
		s.readyToSendPrepareResultToCoordinator(s.txnStore[txnId].prepareResultOp)
		return
	}

	if s.server.config.GetFastPath() {
		s.server.executor.FastPrepareResult <- s.txnStore[txnId].prepareResultOp
	}

	if !s.server.IsLeader() {
		return
	}

	//Replicates the prepare result to followers.
	replicationMsg := ReplicationMsg{
		TxnId:             txnId,
		Status:            s.txnStore[txnId].status,
		MsgType:           PrepareResultMsg,
		IsFromCoordinator: false,
	}

	if replicationMsg.Status == PREPARED {
		replicationMsg.PreparedReadKeyVersion = s.txnStore[txnId].prepareResultOp.Request.ReadKeyVerList
		replicationMsg.PreparedWriteKeyVersion = s.txnStore[txnId].prepareResultOp.Request.WriteKeyVerList
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replicationMsg); err != nil {
		log.Errorf("replication encoding error: %v", err)
	}

	log.Debugf("txn %s replicates the prepare result %v.", txnId, s.txnStore[txnId].status)

	s.server.raft.raftInputChannel <- string(buf.Bytes())
}

// check if there is txn can be prepared when key is released
func (s *AbstractStorage) checkPrepare(key string) {
	for s.kvStore[key].WaitingOp.Len() != 0 {
		e := s.kvStore[key].WaitingOp.Front()
		op := e.Value.(*ReadAndPrepareOp)
		txnId := op.txnId
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
				op.txnId, hasWaiting, canPrepare, key)
			break
		}

		log.Infof("can prepare %v when key %v is released", op.txnId, key)
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
		txnId := op.request.txnId
		s.txnStore[txnId].readAndPrepareRequestOp = op.request
		s.setPrepareResult(op.request)
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

func (s *AbstractStorage) replicateCommitResult(txnId string, writeData []*rpc.KeyValue) {
	if !s.server.config.GetReplication() {
		log.Debugf("txn %v config no replication", txnId)
		return
	}
	log.Debugf("txn %v replicate commit result %v", txnId, s.txnStore[txnId].status)

	replicationMsg := ReplicationMsg{
		TxnId:             txnId,
		Status:            s.txnStore[txnId].status,
		MsgType:           CommitResultMsg,
		IsFromCoordinator: false,
		WriteData:         writeData,
		IsFastPathSuccess: s.txnStore[txnId].isFastPrepare,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replicationMsg); err != nil {
		log.Errorf("replication encoding error: %v", err)
	}

	s.server.raft.raftInputChannel <- string(buf.Bytes())
}

func (s *AbstractStorage) selfAbort(op *ReadAndPrepareOp) {
	txnId := op.txnId
	log.Debugf("txn %v passed timestamp also cannot prepared", txnId)
	s.setReadResult(op)
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
		s.replicateCommitResult(txnId, nil)
	}
}

func (s *AbstractStorage) initTxnIfNotExist(msg ReplicationMsg) bool {
	if _, exist := s.txnStore[msg.TxnId]; !exist {
		s.txnStore[msg.TxnId] = &TxnInfo{
			readAndPrepareRequestOp: nil,
			prepareResultOp:         nil,
			status:                  INIT,
			receiveFromCoordinator:  false,
			sendToClient:            false,
			sendToCoordinator:       false,
			commitOrder:             0,
			waitingTxnKey:           0,
			waitingTxnDep:           0,
			startTime:               time.Now(),
			preparedTime:            time.Now(),
			commitTime:              time.Now(),
			canReorder:              0,
		}
		if msg.Status == PREPARED {
			s.txnStore[msg.TxnId].readAndPrepareRequestOp = NewReadAndPrepareOpWithReplicatedMsg(msg, s.server)
			s.txnStore[msg.TxnId].prepareResultOp = NewPrepareRequestOpWithReplicatedMsg(s.server.partitionId, msg)
		}
		return false
	}
	return true
}

func (s *AbstractStorage) ApplyReplicationMsg(msg ReplicationMsg) {
	switch msg.MsgType {
	case PrepareResultMsg:
		log.Debugf("txn %v apply prepare result %v", msg.TxnId, msg.Status)
		if s.server.IsLeader() {
			s.readyToSendPrepareResultToCoordinator(s.txnStore[msg.TxnId].prepareResultOp)
			break
		}
		s.initTxnIfNotExist(msg)
		if !s.server.config.GetFastPath() {
			if s.txnStore[msg.TxnId].status != INIT {
				log.Fatalf("txn %v in follower should be INIT but it is %v", msg.TxnId, s.txnStore[msg.TxnId].status)
			}
			s.txnStore[msg.TxnId].status = msg.Status
			break
		}
		s.abstractMethod.applyReplicatedPrepareResult(msg)

		break
	case CommitResultMsg:
		log.Debugf("txn %v apply commit result %v", msg.TxnId, msg.Status)
		if s.server.IsLeader() {
			break
		}
		s.initTxnIfNotExist(msg)
		if !s.server.config.GetFastPath() {
			log.Debugf("txn %v apply commit result disable fast path", msg.TxnId)
			s.txnStore[msg.TxnId].status = msg.Status
			if msg.Status == COMMIT {
				s.txnStore[msg.TxnId].commitOrder = s.committed
				s.committed++
				s.txnStore[msg.TxnId].commitTime = time.Now()
				s.writeToDB(msg.WriteData)
				s.print()
			}
			break
		}
		s.abstractMethod.applyReplicatedCommitResult(msg)

		break
	default:
		log.Fatalf("invalid msg type %v", msg.Status)
		break
	}
}
