package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
	"time"
)

type ReadAndPrepareGTS struct {
	txnId string

	// client prepareResult
	request *rpc.ReadAndPrepareRequest
	// read result will send to client
	reply *rpc.ReadAndPrepareReply
	// client will block on this chan until read prepareResult is ready
	clientWait chan bool

	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.

	// keys in this partition
	keyMap map[string]bool
	// include keys in other partition
	allKeys      map[string]bool
	allReadKeys  map[string]bool
	allWriteKeys map[string]bool

	// indicate the current time is greater than the timestamp in txn
	passedTimestamp bool

	// true: there is a conflict high priority txn within. only low priority txn will selfAbort
	selfAbort bool

	// for
	probeWait chan bool
	probe     bool

	highPriority bool
}

func NewReadAndPrepareOpWithReplicatedMsg(msg ReplicationMsg, server *Server) *ReadAndPrepareGTS {
	r := &ReadAndPrepareGTS{
		request:         nil,
		clientWait:      nil,
		index:           0,
		reply:           nil,
		keyMap:          make(map[string]bool),
		allKeys:         make(map[string]bool),
		allReadKeys:     make(map[string]bool),
		allWriteKeys:    make(map[string]bool),
		passedTimestamp: false,
		txnId:           msg.TxnId,
		highPriority:    msg.HighPriority,
	}
	readKeyList := make([]string, len(msg.PreparedReadKeyVersion))
	for i, kv := range msg.PreparedReadKeyVersion {
		readKeyList[i] = kv.Key
	}
	writeKeyList := make([]string, len(msg.PreparedWriteKeyVersion))
	for i, kv := range msg.PreparedWriteKeyVersion {
		writeKeyList[i] = kv.Key
	}
	r.processKey(readKeyList, server, READ)
	r.processKey(writeKeyList, server, WRITE)

	return r
}

func NewReadAndPrepareGTS(request *rpc.ReadAndPrepareRequest, server *Server) *ReadAndPrepareGTS {
	r := &ReadAndPrepareGTS{
		request:         request,
		clientWait:      make(chan bool, 1),
		index:           0,
		reply:           nil,
		keyMap:          make(map[string]bool),
		allKeys:         make(map[string]bool),
		passedTimestamp: false,
		txnId:           request.Txn.TxnId,
		allReadKeys:     make(map[string]bool),
		allWriteKeys:    make(map[string]bool),
		highPriority:    request.Txn.HighPriority,
	}

	r.processKey(request.Txn.ReadKeyList, server, READ)
	r.processKey(request.Txn.WriteKeyList, server, WRITE)

	return r
}

func (o *ReadAndPrepareGTS) processKey(keys []string, server *Server, keyType KeyType) {
	for _, key := range keys {
		o.allKeys[key] = true
		if keyType == WRITE {
			o.allWriteKeys[key] = false
		} else if keyType == READ {
			o.allReadKeys[key] = false
		}

		if !server.storage.HasKey(key) {
			continue
		}
		o.keyMap[key] = true
	}
}

func (o *ReadAndPrepareGTS) Execute(storage *Storage) {
	log.Debugf("txn %v start execute", o.txnId)
	if o.highPriority {
		o.executeHighPriority(storage)
	} else {
		o.executeLowPriority(storage)
	}
}

func (o *ReadAndPrepareGTS) executeHighPriority(storage *Storage) {
	if storage.checkAbort(o) {
		log.Debugf("txn %v high prioirty txn already abort", o.txnId)
		storage.setReadResult(o, -1, false)
		return
	}

	storage.AddTxn(o)

	available := storage.checkKeysAvailable(o)
	waiting := storage.hasWaitingTxn(o)

	if available && !waiting {
		storage.prepare(o)
	} else if available {
		storage.reorderPrepare(o)
	} else if !waiting {
		storage.conditionalPrepare(o)
	} else {
		storage.wait(o)
	}
}

func (o *ReadAndPrepareGTS) executeFromQueue(storage *Storage) bool {

	waiting := storage.hasWaitingTxn(o)
	if waiting {
		log.Debugf("txn %v still has txn before it, wait", o.txnId)
		return false
	}

	available, reorderTxn := storage.checkKeysAvailableFromQueue(o)

	if available && len(reorderTxn) == 0 {
		storage.prepare(o)
	} else if available {
		storage.reverseReorderPrepare(o, reorderTxn)
	} else {
		return false
	}

	return true
}

func (o *ReadAndPrepareGTS) executeLowPriority(storage *Storage) {
	log.Debugf("txn %v execute low priority")

	if storage.checkAbort(o) {
		log.Debugf("txn %v is ready abort", o.txnId)
		storage.setReadResult(o, -1, false)
		return
	}

	storage.AddTxn(o)
	storage.setReadResult(o, -1, false)

	// first check if keys are available and if there is waiting txn (only high priority txn will wait)
	if o.selfAbort {
		log.Debugf("txn %v is already self abort by scheduler", o.txnId)
		storage.selfAbort(o)
		return
	}

	available := storage.checkKeysAvailable(o)
	if !available {
		log.Debugf("txn %v key is not available, abort", o.txnId)
		storage.selfAbort(o)
	}
	wait := storage.hasWaitingTxn(o)
	if wait {
		log.Debugf("txn %v there is waiting txn, abort", o.txnId)
		storage.selfAbort(o)
	}

	log.Debugf("txn %v prepared", o.txnId)
	storage.prepare(o)
}

func (o *ReadAndPrepareGTS) Schedule(scheduler *Scheduler) {
	if o.request.Timestamp < time.Now().UnixNano() {
		log.Infof("PASS Current time %v", o.txnId)
		o.passedTimestamp = true
	}

	scheduler.priorityQueue.Push(o)
	if o.highPriority && scheduler.server.config.GetPriority() && scheduler.server.config.IsEarlyAbort() {
		scheduler.highPrioritySL.Insert(o, o.request.Timestamp)
	}
	if o.index == 0 {
		if !scheduler.timer.Stop() && len(scheduler.timer.C) > 0 {
			<-scheduler.timer.C
		}
		scheduler.resetTimer()
	}
}

func (o *ReadAndPrepareGTS) GetReadKeys() []string {
	return o.request.Txn.ReadKeyList
}

func (o *ReadAndPrepareGTS) GetWriteKeys() []string {
	return o.request.Txn.WriteKeyList
}

func (o *ReadAndPrepareGTS) GetTxnId() string {
	return o.request.Txn.TxnId
}

func (o *ReadAndPrepareGTS) GetPriority() bool {
	return o.request.Txn.HighPriority
}

func (o *ReadAndPrepareGTS) GetReadReply() *rpc.ReadAndPrepareReply {
	return o.reply
}

func (o *ReadAndPrepareGTS) GetKeyMap() map[string]bool {
	return o.keyMap
}

func (o *ReadAndPrepareGTS) SetReadReply(reply *rpc.ReadAndPrepareReply) {
	o.reply = reply
}

func (o *ReadAndPrepareGTS) UnblockClient() {
	o.clientWait <- true
}

func (o *ReadAndPrepareGTS) BlockClient() {
	<-o.clientWait
}

func (o *ReadAndPrepareGTS) GetCoordinatorPartitionId() int {
	return int(o.request.Txn.CoordPartitionId)
}

func (o *ReadAndPrepareGTS) GetReadRequest() *rpc.ReadAndPrepareRequest {
	return o.request
}

func (o *ReadAndPrepareGTS) GetTimestamp() int64 {
	return o.request.Timestamp
}
