package server

import (
	"Carousel-GTS/rpc"
	log "github.com/sirupsen/logrus"
	"time"
)

type ReadAndPrepareGTS struct {
	*ReadAndPrepare2PL

	// include keys in other partition
	allKeys      map[string]bool
	allReadKeys  map[string]bool
	allWriteKeys map[string]bool

	// indicate the current time is greater than the timestamp in txn
	passedTimestamp bool

	// true: there is a conflict high priority txn within. only low priority txn will selfAbort
	selfAbort bool
}

func NewReadAndPrepareGTSWithReplicatedMsg(msg *ReplicationMsg) *ReadAndPrepareGTS {
	r := &ReadAndPrepareGTS{
		ReadAndPrepare2PL: NewReadAndPrepare2PLWithReplicationMsg(msg),
		allKeys:           make(map[string]bool),
		allReadKeys:       make(map[string]bool),
		allWriteKeys:      make(map[string]bool),
		passedTimestamp:   false,
	}

	return r
}

func NewReadAndPrepareGTSProbe() *ReadAndPrepareGTS {
	r := &ReadAndPrepareGTS{
		ReadAndPrepare2PL: &ReadAndPrepare2PL{},
		allKeys:           nil,
		allReadKeys:       nil,
		allWriteKeys:      nil,
		passedTimestamp:   false,
		selfAbort:         false,
	}
	r.clientWait = make(chan bool, 1)
	return r
}

func NewReadAndPrepareGTS(request *rpc.ReadAndPrepareRequest, server *Server) *ReadAndPrepareGTS {
	r := &ReadAndPrepareGTS{
		ReadAndPrepare2PL: NewReadAndPrepareLock2PL(request),
		allKeys:           make(map[string]bool),
		passedTimestamp:   false,
		allReadKeys:       make(map[string]bool),
		allWriteKeys:      make(map[string]bool),
	}

	if server.config.IsEarlyAbort() {
		r.keyMap = make(map[string]bool)
		r.readKeyList = make([]string, 0)
		r.writeKeyList = make([]string, 0)

		r.processKey(request.Txn.ReadKeyList, server, READ)
		r.processKey(request.Txn.WriteKeyList, server, WRITE)
	}

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

		if keyType == WRITE {
			o.writeKeyList = append(o.writeKeyList, key)
		} else if keyType == READ {
			o.readKeyList = append(o.readKeyList, key)
		}
		o.keyMap[key] = true
	}
}

func (o *ReadAndPrepareGTS) Execute(storage *Storage) {
	log.Debugf("txn %v start execute timestamp %v idx %v",
		o.txnId, o.request.Timestamp, o.index)
	if o.highPriority {
		o.executeHighPriority(storage)
	} else {
		o.executeLowPriority(storage)
	}
}

func (o *ReadAndPrepareGTS) executeHighPriority(storage *Storage) {
	log.Debugf("txn %v execute high priority", o.txnId)

	if storage.checkAbort(o) {
		log.Debugf("txn %v high prioirty txn already abort", o.txnId)
		storage.setReadResult(o, -1, false)
		return
	}

	storage.AddTxn(o)

	available := storage.checkKeysAvailable(o)
	waiting := storage.hasWaitingTxn(o)

	if available && !waiting {
		storage.setReadResult(o, -1, false)
		storage.prepare(o)
	} else if available {
		storage.reorderPrepare(o)
	} else if !waiting {
		storage.conditionalPrepare(o)
	} else {
		if o.passedTimestamp {
			storage.setReadResult(o, -1, false)
			storage.selfAbort(o, PASS_TIMESTAMP_ABORT)
			return
		}

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
		storage.setReadResult(o, -1, false)
		storage.prepare(o)
	} else if available {
		storage.setReadResult(o, -1, false)
		storage.reverseReorderPrepare(o, reorderTxn)
	} else {
		return false
	}

	storage.removeFromQueue(o)

	return true
}

func (o *ReadAndPrepareGTS) executeLowPriority(storage *Storage) {
	log.Debugf("txn %v execute low priority", o.txnId)

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
		storage.selfAbort(o, EARLY_ABORT)
		return
	}

	available := storage.checkKeysAvailable(o)
	if !available {
		log.Debugf("txn %v key is not available, abort", o.txnId)
		storage.selfAbort(o, CONFLICT_ABORT)
		return
	}
	wait := storage.hasWaitingTxn(o)
	if wait {
		log.Debugf("txn %v there is waiting txn, abort", o.txnId)
		storage.selfAbort(o, WAITING_ABORT)
		return
	}

	log.Debugf("txn %v prepared", o.txnId)
	storage.prepare(o)
}

func (o *ReadAndPrepareGTS) Schedule(scheduler *Scheduler) {
	if o.request.Timestamp < time.Now().UnixNano() {
		log.Debugf("PASS Current time %v", o.txnId)
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

func (o *ReadAndPrepareGTS) GetAllWriteKeys() map[string]bool {
	return o.allWriteKeys
}

func (o *ReadAndPrepareGTS) GetAllReadKeys() map[string]bool {
	return o.allReadKeys
}

func (o *ReadAndPrepareGTS) setSelfAbort() {
	o.selfAbort = true
}

func (o *ReadAndPrepareGTS) IsPassTimestamp() bool {
	return o.passedTimestamp
}

func (o *ReadAndPrepareGTS) IsSelfAbort() bool {
	return o.selfAbort
}

//func (o *ReadAndPrepareGTS) GetTimestamp() int64 {
//	return o.request.Timestamp
//}

func (o *ReadAndPrepareGTS) Start(server *Server) {
	server.scheduler.AddOperation(o)
}
