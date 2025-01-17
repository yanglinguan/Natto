package server

import (
	"Carousel-GTS/utils"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
)

type KeyType int

const (
	READ KeyType = iota
	WRITE
)

type KeyInfo struct {
	Value            string
	Version          uint64
	WaitingQueue     WaitingList
	PreparedTxnRead  map[string]bool // true: high priority; false: low Priority
	PreparedTxnWrite map[string]bool // true: high priority; false: low Priority

	// for timestamp ordering
	readTS  int64
	writeTS int64
}

func newKeyInfoWithPriorityQueue(value string) *KeyInfo {
	k := &KeyInfo{
		Value: value,
		//Version:          uint64(utils.ConvertToInt(value)),
		Version:          0,
		WaitingQueue:     NewPQueue(),
		PreparedTxnRead:  make(map[string]bool),
		PreparedTxnWrite: make(map[string]bool),
	}

	return k
}

func newKeyInfoWithQueue(value string) *KeyInfo {
	k := &KeyInfo{
		Value: value,
		//Version:          uint64(utils.ConvertToInt(value)),
		Version:          0,
		WaitingQueue:     NewQueue(),
		PreparedTxnRead:  make(map[string]bool),
		PreparedTxnWrite: make(map[string]bool),
	}
	return k
}

// Not thread safe
type KVStore struct {
	keys   map[string]*KeyInfo
	server *Server
}

func NewKVStore(server *Server) *KVStore {
	kvStore := &KVStore{
		keys:   make(map[string]*KeyInfo),
		server: server,
	}
	return kvStore
}

func (kv *KVStore) Position(key string, txnId string) int {
	return kv.keys[key].WaitingQueue.Position(txnId)
}

// add key value pair
func (kv *KVStore) AddKeyValue(key string, value string) {
	//if kv.server.config.IsOptimisticReorder() ||
	//	(kv.server.config.GetServerMode() == configuration.TwoPL && !kv.server.config.UseNetworkTimestamp()) {
	if kv.server.config.IsOptimisticReorder() {
		kv.keys[key] = newKeyInfoWithPriorityQueue(value)
	} else {
		kv.keys[key] = newKeyInfoWithQueue(value)
	}
	//kv.keys[key] = newKeyInfo(value, kv.server.config.IsOptimisticReorder() || kv.server.config.GetServerMode() == configuration.TwoPL)
}

// check if key exists
func (kv KVStore) ContainsKey(key string) bool {
	_, exist := kv.keys[key]
	return exist
}

// get value and version
// return error if key does not exist
func (kv *KVStore) Get(key string) (string, uint64) {
	kv.checkExistHandleKeyNotExistError(key)
	return kv.keys[key].Value, kv.keys[key].Version
}

// update key value pair
// if key does not exist, create a new KeyInfo
func (kv *KVStore) Put(key string, value string) {
	if _, exist := kv.keys[key]; exist {
		kv.keys[key].Value = value
		kv.keys[key].Version = uint64(utils.ConvertToInt(value))
	} else {
		kv.keys[key] = newKeyInfoWithQueue(value)
		//kv.keys[key] = newKeyInfo(value, kv.server.config.IsOptimisticReorder())
	}
}

func (kv *KVStore) UpdateTimestamp(key string, rTS int64, wTS int64) {
	kv.keys[key].readTS = rTS
	kv.keys[key].writeTS = wTS
}

// add keys to waiting list
func (kv *KVStore) AddToWaitingList(op LockingOp) int {
	maxQueueLen := 0
	log.Debugf("txn %v add to waiting queue", op.GetTxnId())
	for key := range op.GetKeyMap() {
		kv.checkExistHandleKeyNotExistError(key)
		qLen := kv.keys[key].WaitingQueue.Len()
		if qLen > maxQueueLen {
			maxQueueLen = qLen
		}
		kv.keys[key].WaitingQueue.Push(op)
		log.Debugf("txn %v wait on key %v idx %v timestamp %v current top %v ",
			op.GetTxnId(), key, op.GetIndex(), op.GetTimestamp(), kv.keys[key].WaitingQueue.Front().GetTxnId())
		//item := kv.keys[key].waitingOp.PushBack(op)
		//kv.keys[key].waitingItem[op.txnId] = item
	}

	return maxQueueLen
}

//func (kv *KVStore) WaitingOnKey(op LockingOp, key string) {
//	kv.keys[key].WaitingQueue.Push(op)
//}

// remove txn from the waiting list
func (kv *KVStore) RemoveFromWaitingList(op LockingOp) {
	// only high priority will wait
	//if !op.GetPriority() {
	//	return
	//}
	for key := range op.GetKeyMap() {
		//kv.checkExistHandleKeyNotExistError(key)

		removed := kv.keys[key].WaitingQueue.Remove(op)
		log.Debugf("txn %v remove from queue key %v removed %v", op.GetTxnId(), key, removed)
	}
}

func (kv *KVStore) isTop(txnId string, key string) bool {
	if !kv.keys[key].WaitingQueue.InQueue(txnId) {
		return true
	}
	front := kv.keys[key].WaitingQueue.Front()
	if front == nil {
		return true
	}
	return front.GetTxnId() == txnId
}

//func (kv *KVStore) removeFromQueue(op PriorityOp) {
//
//	kv.keys[key].WaitingQueue.Remove(op)
//}

// mark prepared keys
func (kv *KVStore) RecordPrepared(op ReadAndPrepareOp) {
	txnId := op.GetTxnId()
	for rk := range op.GetReadKeys() {
		kv.checkExistHandleKeyNotExistError(rk)
		//op.readKeyMap[rk] = true
		kv.keys[rk].PreparedTxnRead[txnId] = op.GetPriority()
	}
	for wk := range op.GetWriteKeys() {
		kv.checkExistHandleKeyNotExistError(wk)
		//op.writeKeyMap[wk] = true
		kv.keys[wk].PreparedTxnWrite[txnId] = op.GetPriority()
	}
}

func (kv *KVStore) ReleaseKeys(op ReadAndPrepareOp) {
	txnId := op.GetTxnId()
	for rk := range op.GetReadKeys() {
		kv.checkExistHandleKeyNotExistError(rk)
		log.Debugf("txn %v release read key %v", txnId, rk)
		delete(kv.keys[rk].PreparedTxnRead, txnId)
	}

	for wk := range op.GetWriteKeys() {
		kv.checkExistHandleKeyNotExistError(wk)
		log.Debugf("txn %v release write key %v", txnId, wk)
		delete(kv.keys[wk].PreparedTxnWrite, txnId)
	}
}

func (kv *KVStore) checkExistHandleKeyNotExistError(key string) {
	if _, exist := kv.keys[key]; !exist {
		log.Fatalf("key %v does not exist", key)
	}
}

// return true if any txn hold write lock of key
func (kv *KVStore) IsTxnHoldWrite(key string) bool {
	kv.checkExistHandleKeyNotExistError(key)
	log.Debugf("key %v write hold by %v", key, kv.keys[key].PreparedTxnWrite)
	return len(kv.keys[key].PreparedTxnWrite) > 0
}

// return true if any txn hold read lock of key
func (kv *KVStore) IsTxnHoldRead(key string) bool {
	kv.checkExistHandleKeyNotExistError(key)
	log.Debugf("key %v read hold by %v", key, kv.keys[key].PreparedTxnRead)
	return len(kv.keys[key].PreparedTxnRead) > 0
}

// return true if any high priority txn hold write lock of key
func (kv *KVStore) IsHighTxnHoldWrite(key string) bool {
	kv.checkExistHandleKeyNotExistError(key)
	for _, priority := range kv.keys[key].PreparedTxnWrite {
		if priority {
			return true
		}
	}

	return false
}

func (kv *KVStore) GetReadTS(key string) int64 {
	return kv.keys[key].readTS
}

func (kv *KVStore) GetWriteTS(key string) int64 {
	return kv.keys[key].writeTS
}

// return true if any high priority txn hold read lock of key
func (kv *KVStore) IsHighTxnHoldRead(key string) bool {
	kv.checkExistHandleKeyNotExistError(key)
	for _, priority := range kv.keys[key].PreparedTxnRead {
		if priority {
			return true
		}
	}

	return false
}

// return true if any low priority txn hold write lock of key
func (kv *KVStore) IsLowTxnHoldWrite(key string) bool {
	kv.checkExistHandleKeyNotExistError(key)
	for _, priority := range kv.keys[key].PreparedTxnWrite {
		if !priority {
			return true
		}
	}
	return false
}

// return true if any low priority txn hold read lock of key
func (kv *KVStore) IsLowTxnHoldRead(key string) bool {
	kv.checkExistHandleKeyNotExistError(key)
	for _, priority := range kv.keys[key].PreparedTxnRead {
		if !priority {
			return true
		}
	}
	return false
}

// return txn hold write lock
func (kv *KVStore) GetTxnHoldWrite(key string) map[string]bool {
	kv.checkExistHandleKeyNotExistError(key)
	return kv.keys[key].PreparedTxnWrite
}

// return list of txn hold read lock
func (kv *KVStore) GetTxnHoldRead(key string) map[string]bool {
	kv.checkExistHandleKeyNotExistError(key)
	return kv.keys[key].PreparedTxnRead
}

func (kv *KVStore) HasWaitingTxn(op LockingOp) bool {
	//waiting := false
	for key := range op.GetKeyMap() {
		//kv.checkExistHandleKeyNotExistError(key)
		if kv.keys[key].WaitingQueue.Len() > 0 {
			//for txnId, w := range kv.keys[key].WaitingQueue.GetWaitingItems() {
			//	log.Debugf("txn %v key %v waiting txn %v idx %v timestamp %v",
			//		op.GetTxnId(), key, txnId, w.GetIndex(), w.GetTimestamp())
			//}

			top := kv.keys[key].WaitingQueue.Front()
			if top.GetTxnId() != op.GetTxnId() {
				log.Debugf("txn %v has txn in queue key %v top of queue is %v",
					op.GetTxnId(), key, top.GetTxnId())
				return true
				//waiting = true
			}
		}
	}

	//return waiting
	return false
}

func (kv *KVStore) GetNextWaitingTxn(key string) LockingOp {
	kv.checkExistHandleKeyNotExistError(key)
	if kv.keys[key].WaitingQueue.Len() > 0 {
		e := kv.keys[key].WaitingQueue.Front()
		log.Debugf("txn %v is the next wait txn for key %v", e.GetTxnId(), key)
		return e
	}
	log.Debugf("key %v does not have wait txn", key)
	return nil
}

func (kv *KVStore) finalWaitStateCheck() {
	for key, kv := range kv.keys {
		if len(kv.PreparedTxnRead) != 0 ||
			len(kv.PreparedTxnWrite) != 0 ||
			kv.WaitingQueue.Len() != 0 {
			for rt := range kv.PreparedTxnRead {
				log.Errorf("txn %v prepared for read key %v", rt, key)
			}
			for wt := range kv.PreparedTxnWrite {
				log.Errorf("txn %v prepared for write key %v", wt, key)
			}
			//for txn := range kv.waitingItem {
			//	log.Errorf("txn %v is clientWait for key %v", txn, key)
			//}
			log.Fatalf("key %v should have waiting txn", key)
		}
	}
}

func (kv *KVStore) printModifiedData(fileName string) {
	file, err := os.Create(fileName)
	if err != nil || file == nil {
		log.Fatal("Fails to create log file: statistic.log")
		return
	}

	_, err = file.WriteString("#key, value, version\n")
	if err != nil {
		log.Fatalf("Cannot write to file, %v", err)
		return
	}

	for key, kv := range kv.keys {
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
