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
	CONDITIONAL_PREPARED
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
	//HighPriority            bool
}

type TxnInfo struct {
	readAndPrepareRequestOp          *ReadAndPrepareOp
	prepareResultOp                  *PrepareResultOp
	status                           TxnStatus
	receiveFromCoordinator           bool
	sendToClient                     bool
	sendToCoordinator                bool
	commitOrder                      int
	waitingTxnKey                    int
	waitingTxnDep                    int
	startTime                        time.Time
	preparedTime                     time.Time
	commitTime                       time.Time
	canReorder                       int
	isFastPrepare                    bool
	inQueue                          bool
	hasWaitingButNoWriteReadConflict bool
}

type KeyInfo struct {
	Value                       string
	Version                     uint64
	WaitingOp                   *list.List
	WaitingItem                 map[string]*list.Element
	PreparedTxnRead             map[string]bool
	PreparedTxnWrite            map[string]bool
	PreparedLowPriorityTxnRead  map[string]bool
	PreparedLowPriorityTxnWrite map[string]bool
}

type Storage interface {
	Prepare(op *ReadAndPrepareOp)
	Commit(op *CommitRequestOp)
	Abort(op *AbortRequestOp)
	LoadKeys(keys []string)
	PrintStatus(op *PrintStatusRequestOp)
	HasKey(key string) bool
	ApplyReplicationMsg(msg ReplicationMsg)
	ReleaseReadOnly(op *ReadAndPrepareOp)
	AddHighPriorityTxn(op *ReadAndPrepareOp)
}

type AbstractMethod interface {
	//checkKeysAvailable(op *ReadAndPrepareOp) bool
	checkKeysAvailableForHighPriorityTxn(op *ReadAndPrepareOp) (bool, map[int]bool)
	prepared(op *ReadAndPrepareOp, condition map[int]bool)
	abortProcessedTxn(txnId string)
	applyReplicatedPrepareResult(msg ReplicationMsg)
	applyReplicatedCommitResult(msg ReplicationMsg)
}

type PriorityTxnInfo struct {
	lowPriorityTxnRead   map[string]bool
	lowPriorityTxnWrite  map[string]bool
	highPriorityTxnRead  map[string]bool
	highPriorityTxnWrite map[string]bool
}

func NewPriorityTxnInfo() *PriorityTxnInfo {
	return &PriorityTxnInfo{
		lowPriorityTxnRead:   make(map[string]bool),
		lowPriorityTxnWrite:  make(map[string]bool),
		highPriorityTxnRead:  make(map[string]bool),
		highPriorityTxnWrite: make(map[string]bool),
	}
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

	// reorder the high priority txn at index 0, low priority txn at index 1
	otherPartitionKey map[string]*PriorityTxnInfo

	highPriorityTxnQueue *list.List
	highPriorityTxnMap   map[string]*list.Element
}

func NewAbstractStorage(server *Server) *AbstractStorage {
	s := &AbstractStorage{
		kvStore:              make(map[string]*KeyInfo),
		server:               server,
		txnStore:             make(map[string]*TxnInfo),
		committed:            0,
		totalCommit:          0,
		otherPartitionKey:    make(map[string]*PriorityTxnInfo),
		highPriorityTxnQueue: list.New(),
		highPriorityTxnMap:   make(map[string]*list.Element),
	}

	return s
}

func (s *AbstractStorage) LoadKeys(keys []string) {
	for _, key := range keys {
		s.kvStore[key] = &KeyInfo{
			Value:                       key,
			Version:                     0,
			WaitingOp:                   list.New(),
			WaitingItem:                 make(map[string]*list.Element),
			PreparedTxnRead:             make(map[string]bool),
			PreparedTxnWrite:            make(map[string]bool),
			PreparedLowPriorityTxnWrite: make(map[string]bool),
			PreparedLowPriorityTxnRead:  make(map[string]bool),
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
		line := ""
		if s.server.IsLeader() {
			line = fmt.Sprintf("%v %v %v %v %v %v %v %v %v %v\n",
				txnId[i],
				info.waitingTxnKey,
				info.waitingTxnDep,
				info.preparedTime.Sub(info.startTime).Nanoseconds(),
				info.commitTime.Sub(info.preparedTime).Nanoseconds(),
				info.commitTime.Sub(info.startTime).Nanoseconds(),
				info.canReorder,
				info.isFastPrepare,
				info.readAndPrepareRequestOp.request.Timestamp,
				info.hasWaitingButNoWriteReadConflict,
			)
		} else {
			if info == nil {
				log.Warnf("txn %v info %v order %v total commit %v, len %v",
					txnId[i], info, i, s.totalCommit, len(s.txnStore))
			} else {

				line = fmt.Sprintf("%v %v %v %v %v %v %v\n",
					txnId[i],
					info.waitingTxnKey,
					info.waitingTxnDep,
					info.canReorder,
					info.isFastPrepare,
					0,
					info.hasWaitingButNoWriteReadConflict,
				)
			}
		}
		_, err = file.WriteString(line)
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
		Status:        -1,
		IsLeader:      s.server.IsLeader(),
	}

	if s.server.config.GetIsReadOnly() && op.request.Txn.ReadOnly {
		op.reply.Status = int32(s.txnStore[op.txnId].status)
	}

	if op.reply.Status != int32(ABORT) {
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
func (s *AbstractStorage) setPrepareResult(op *ReadAndPrepareOp, condition map[int]bool) {
	txnId := op.txnId
	if _, exist := s.txnStore[txnId]; !exist {
		log.Fatalln("txn %v txnInfo should be created, and INIT status", txnId)
	}

	if s.txnStore[txnId].prepareResultOp != nil {
		return
	}
	conditionList := make([]int32, len(condition))
	i := 0
	for pId := range condition {
		conditionList[i] = int32(pId)
		i++
	}

	prepareResult := &rpc.PrepareResultRequest{
		TxnId:           txnId,
		ReadKeyVerList:  make([]*rpc.KeyVersion, 0),
		WriteKeyVerList: make([]*rpc.KeyVersion, 0),
		PartitionId:     int32(s.server.partitionId),
		PrepareStatus:   int32(s.txnStore[txnId].status),
		Conditions:      conditionList,
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
	s.txnStore[txnId].inQueue = true
	log.Infof("Txn %v wait for keys", txnId)
	for key := range keys {
		log.Debugf("Txn %v wait for key %v", txnId, key)
		item := s.kvStore[key].WaitingOp.PushBack(op)
		s.kvStore[key].WaitingItem[txnId] = item
	}
}

func (s *AbstractStorage) releaseKey(txnId string) {
	txnInfo := s.txnStore[txnId]
	highPriority := txnInfo.readAndPrepareRequestOp.request.Txn.HighPriority
	// remove prepared read and write key
	for rk, isPrepared := range txnInfo.readAndPrepareRequestOp.readKeyMap {
		if isPrepared {
			log.Debugf("txn %v release read key %v", txnId, rk)
			if highPriority {
				delete(s.kvStore[rk].PreparedTxnRead, txnId)
			} else {
				delete(s.kvStore[rk].PreparedLowPriorityTxnRead, txnId)
			}
		}
	}

	for wk, isPrepared := range txnInfo.readAndPrepareRequestOp.writeKeyMap {
		if isPrepared {
			log.Debugf("txn %v release write key %v", txnId, wk)
			if highPriority {
				delete(s.kvStore[wk].PreparedTxnWrite, txnId)
			} else {
				delete(s.kvStore[wk].PreparedLowPriorityTxnWrite, txnId)
			}
		}
	}

	for _, rk := range txnInfo.readAndPrepareRequestOp.otherPartitionReadKey {
		if _, exist := s.otherPartitionKey[rk]; !exist {
			continue
		}
		if highPriority {
			delete(s.otherPartitionKey[rk].highPriorityTxnRead, txnId)
		} else {
			delete(s.otherPartitionKey[rk].lowPriorityTxnRead, txnId)
		}
	}

	for _, wk := range txnInfo.readAndPrepareRequestOp.otherPartitionWriteKey {
		if _, exist := s.otherPartitionKey[wk]; !exist {
			continue
		}
		if highPriority {
			delete(s.otherPartitionKey[wk].highPriorityTxnWrite, txnId)
		} else {
			delete(s.otherPartitionKey[wk].lowPriorityTxnWrite, txnId)
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

func (s *AbstractStorage) ReleaseReadOnly(op *ReadAndPrepareOp) {
	for key := range op.keyMap {
		s.checkPrepare(key)
	}
}

func (s *AbstractStorage) overlapPartitions(txnId1 string, txnId2 string) map[int]bool {
	p1 := make(map[int]bool)
	for key := range s.txnStore[txnId1].readAndPrepareRequestOp.allKeys {
		pId := s.server.config.GetPartitionIdByKey(key)
		p1[pId] = true
	}
	result := make(map[int]bool)
	for key := range s.txnStore[txnId2].readAndPrepareRequestOp.allKeys {
		pId := s.server.config.GetPartitionIdByKey(key)
		if _, exist := p1[pId]; exist {
			result[pId] = true
		}
	}

	log.Debugf("txn %v and txn %v overlap partition %v", txnId1, txnId2, result)

	return result
}

func (s *AbstractStorage) findOverlapPartitionsWithLowPriorityTxn(op *ReadAndPrepareOp) map[int]bool {
	overlapPartition := make(map[int]bool)
	conflictLowPriorityTxn := make(map[string]bool)
	for rk := range op.readKeyMap {
		for txnId := range s.kvStore[rk].PreparedLowPriorityTxnWrite {
			conflictLowPriorityTxn[txnId] = true
		}
	}

	for wk := range op.writeKeyMap {
		for txnId := range s.kvStore[wk].PreparedLowPriorityTxnWrite {
			conflictLowPriorityTxn[txnId] = true
		}
		for txnId := range s.kvStore[wk].PreparedLowPriorityTxnRead {
			conflictLowPriorityTxn[txnId] = true
		}
	}

	for _, rk := range op.otherPartitionReadKey {
		if _, exist := s.otherPartitionKey[rk]; exist {
			for txnId := range s.otherPartitionKey[rk].lowPriorityTxnWrite {
				conflictLowPriorityTxn[txnId] = true
			}
		}
	}

	for _, wk := range op.otherPartitionWriteKey {
		if _, exist := s.otherPartitionKey[wk]; exist {
			for txnId := range s.otherPartitionKey[wk].lowPriorityTxnWrite {
				conflictLowPriorityTxn[txnId] = true
			}

			for txnId := range s.otherPartitionKey[wk].lowPriorityTxnRead {
				conflictLowPriorityTxn[txnId] = true
			}
		}
	}
	log.Debugf("txn %v conflict low priority txn %v", op.txnId, conflictLowPriorityTxn)
	for txnId := range conflictLowPriorityTxn {
		pIdMap := s.overlapPartitions(op.txnId, txnId)
		for pId := range pIdMap {
			overlapPartition[pId] = true
		}
	}

	return overlapPartition
}

func (s *AbstractStorage) checkKeysAvailableForLowPriorityTxn(op *ReadAndPrepareOp) bool {
	// check if there is a high priority txn with in 10ms
	if s.highPriorityTxnQueue.Len() > 0 {
		highTxn := s.highPriorityTxnQueue.Front().Value.(*ReadAndPrepareOp)
		hTm := time.Unix(highTxn.request.Timestamp, 0)
		lTm := time.Unix(op.request.Timestamp, 0)
		if lTm.Sub(hTm) < s.server.config.GetTimeWindow() {
			log.Warnf("txn %v is low priority within %vms there is a high priority txn %v",
				op.txnId, 10, highTxn.txnId)
			return false
		}
	}

	for rk := range op.readKeyMap {
		if len(s.kvStore[rk].PreparedLowPriorityTxnWrite) > 0 || len(s.kvStore[rk].PreparedTxnWrite) > 0 {
			log.Debugf("txn %v (read) : there is txn holding (write) hold key %v", op.txnId, rk)
			return false
		}
	}

	for wk := range op.writeKeyMap {
		if len(s.kvStore[wk].PreparedTxnWrite) > 0 || len(s.kvStore[wk].PreparedLowPriorityTxnWrite) > 0 ||
			len(s.kvStore[wk].PreparedTxnRead) > 0 || len(s.kvStore[wk].PreparedLowPriorityTxnRead) > 0 {
			log.Debugf("txn %v (write) : there is txn hold key %v", op.txnId, wk)
			return false
		}
	}

	for _, rk := range op.otherPartitionReadKey {
		if _, exist := s.otherPartitionKey[rk]; exist {
			if len(s.otherPartitionKey[rk].highPriorityTxnWrite) > 0 {
				log.Debugf("txn %v (read) : there is high priority txn (other partition) conflict key %v",
					op.txnId, rk)
				return false
			}
		}
	}

	for _, wk := range op.otherPartitionWriteKey {
		if _, exist := s.otherPartitionKey[wk]; exist {
			if len(s.otherPartitionKey[wk].highPriorityTxnWrite) > 0 ||
				len(s.otherPartitionKey[wk].highPriorityTxnRead) > 0 {
				log.Debugf("txn %v (write) : there is high priority txn (other partition) conflict key %v",
					op.txnId, wk)
				return false
			}
		}
	}

	return true
}

// check if all keys are available
//func (s *AbstractStorage) checkKeysAvailable(op *ReadAndPrepareOp) bool {
//	available := true
//	for rk := range op.readKeyMap {
//		if len(s.kvStore[rk].PreparedTxnWrite) > 0 {
//			log.Debugf("txn %v cannot prepare because of cannot get read key %v: %v", op.txnId, rk, s.kvStore[rk].PreparedTxnWrite)
//			available = false
//			break
//		}
//	}
//
//	for wk := range op.writeKeyMap {
//		if !available ||
//			len(s.kvStore[wk].PreparedTxnRead) > 0 ||
//			len(s.kvStore[wk].PreparedTxnWrite) > 0 {
//			log.Debugf("txn %v cannot prepare because of cannot get write key %v: read %v write %v",
//				op.txnId, wk, s.kvStore[wk].PreparedTxnRead, s.kvStore[wk].PreparedTxnWrite)
//			available = false
//			break
//		}
//	}
//
//	return available
//}

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
		if op.request.Txn.HighPriority {
			s.kvStore[rk].PreparedTxnRead[txnId] = true
		} else {
			s.kvStore[rk].PreparedLowPriorityTxnRead[txnId] = true
		}
	}
	for wk := range op.writeKeyMap {
		op.writeKeyMap[wk] = true
		if op.request.Txn.HighPriority {
			s.kvStore[wk].PreparedTxnWrite[txnId] = true
		} else {
			s.kvStore[wk].PreparedLowPriorityTxnWrite[txnId] = true
		}
	}

	for _, rk := range op.otherPartitionReadKey {
		if _, exist := s.otherPartitionKey[rk]; !exist {
			s.otherPartitionKey[rk] = NewPriorityTxnInfo()
		}
		if op.request.Txn.HighPriority {
			s.otherPartitionKey[rk].highPriorityTxnRead[txnId] = true
		} else {
			s.otherPartitionKey[rk].lowPriorityTxnRead[txnId] = true
		}
	}

	for _, wk := range op.otherPartitionWriteKey {
		if _, exist := s.otherPartitionKey[wk]; !exist {
			s.otherPartitionKey[wk] = NewPriorityTxnInfo()
		}
		if op.request.Txn.HighPriority {
			s.otherPartitionKey[wk].highPriorityTxnWrite[txnId] = true
		} else {
			s.otherPartitionKey[wk].lowPriorityTxnWrite[txnId] = true
		}
	}
}

func (s *AbstractStorage) removeFromQueue(op *ReadAndPrepareOp) {
	// remove from the top of the queue
	txnId := op.txnId
	for key := range op.keyMap {
		if _, exist := s.kvStore[key].WaitingItem[txnId]; !exist {
			continue
		}

		s.kvStore[key].WaitingOp.Remove(s.kvStore[key].WaitingItem[txnId])
		delete(s.kvStore[key].WaitingItem, txnId)
	}
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
		//HighPriority:      s.txnStore[txnId].readAndPrepareRequestOp.request.Txn.HighPriority,
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
		hasWaiting := s.hasWaitingTxn(op)
		canPrepare := !hasWaiting
		condition := make(map[int]bool)
		if !hasWaiting {
			canPrepare, condition = s.abstractMethod.checkKeysAvailableForHighPriorityTxn(op)
		}
		if !canPrepare {
			log.Infof("cannot prepare %v had waiting %v, can prepare %v when release key %v",
				op.txnId, hasWaiting, canPrepare, key)
			break
		}

		log.Infof("can prepare %v when key %v is released", op.txnId, key)
		s.abstractMethod.prepared(op, condition)
	}
}

func (s *AbstractStorage) writeToDB(op []*rpc.KeyValue) {
	for _, kv := range op {
		s.kvStore[kv.Key].Value = kv.Value
		s.kvStore[kv.Key].Version++
	}
}

func (s *AbstractStorage) Abort(op *AbortRequestOp) {
	// coordinator abort
	txnId := op.abortRequest.TxnId
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
		if s.server.config.GetPriority() {
			s.removeHighPriorityTxn(txnId)
		}
		s.replicateCommitResult(txnId, nil)
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
		//HighPriority:      s.txnStore[txnId].readAndPrepareRequestOp.request.Txn.HighPriority,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replicationMsg); err != nil {
		log.Errorf("replication encoding error: %v", err)
	}

	s.server.raft.raftInputChannel <- string(buf.Bytes())
}

func (s *AbstractStorage) selfAbort(op *ReadAndPrepareOp) {
	s.setReadResult(op)
	if op.request.Txn.ReadOnly && s.server.config.GetIsReadOnly() {
		return
	}
	s.setPrepareResult(op, make(map[int]bool))
	s.replicatePreparedResult(op.txnId)
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
			//if s.txnStore[msg.TxnId].status != INIT {
			//	log.Fatalf("txn %v in follower should be INIT but it is %v", msg.TxnId, s.txnStore[msg.TxnId].status)
			//}
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

func (s *AbstractStorage) AddHighPriorityTxn(op *ReadAndPrepareOp) {
	e := s.highPriorityTxnQueue.PushBack(op)
	s.highPriorityTxnMap[op.txnId] = e
}

func (s *AbstractStorage) removeHighPriorityTxn(txnId string) {
	if _, exist := s.highPriorityTxnMap[txnId]; !exist {
		log.Debugf("txn %v high priority does not exist in queue", txnId)
		return
	}

	e := s.highPriorityTxnMap[txnId]
	s.highPriorityTxnQueue.Remove(e)
}
