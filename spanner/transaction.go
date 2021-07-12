package spanner

import (
	"bytes"
	"encoding/gob"
	"github.com/sirupsen/logrus"
	"sync"
)

type TxnStatus int32

const (
	INIT TxnStatus = iota
	READ
	WRITE
	PREPARED
	ABORTED
	COMMITTED
)

type transaction struct {
	txnId                string
	coordPId             int
	participantPartition map[int]bool
	timestamp            int64
	clientId             int64
	readKeys             []string

	writeKeys map[string]string

	waitKeys map[string]LockType

	keyMap map[string]bool

	Status   TxnStatus
	finalize bool

	read2PLOp *read2PL

	commitOp *commit2PL

	server            *Server
	replicatedPrepare bool

	// for priority queue
	index int

	// the client lib uses the lock and wait group
	readResult map[string]*ValVer
	lock       sync.Mutex
	wg         sync.WaitGroup

	priority bool
}

func NewTransaction(
	txnId string,
	timestamp int64,
	cId int64,
	priority bool) *transaction {
	t := &transaction{
		txnId:                txnId,
		coordPId:             0,
		participantPartition: make(map[int]bool),
		timestamp:            timestamp,
		clientId:             cId,
		readKeys:             nil,
		writeKeys:            nil,
		waitKeys:             make(map[string]LockType),
		keyMap:               make(map[string]bool),
		Status:               INIT,
		read2PLOp:            nil,
		commitOp:             nil,
		server:               nil,
		index:                0,
		readResult:           make(map[string]*ValVer),
		lock:                 sync.Mutex{},
		priority:             priority,
	}
	return t
}

func (t *transaction) setReadKeys(readKeys []string) {
	t.readKeys = readKeys
}

func (t *transaction) setWriteKeys(writeKeys map[string]string) {
	t.writeKeys = writeKeys
}

// this transaction is older than txn
func (t transaction) isOlderThan(txn *transaction) bool {
	if txn.timestamp == t.timestamp {
		if txn.clientId == t.clientId {
			return t.txnId < txn.txnId
		}
		return t.clientId < txn.clientId
	}
	return t.timestamp < txn.timestamp
}

func (t *transaction) addWaitKey(key string, lockType LockType) {
	t.waitKeys[key] = lockType
}

func (t *transaction) getWaitKey() map[string]LockType {
	return t.waitKeys
}

func (t *transaction) removeWaitKey(key string) {
	delete(t.waitKeys, key)
}

func (t *transaction) replyRead() {
	logrus.Debugf("txn %v reply the read result to client %v, status %v",
		t.txnId, t.clientId, t.Status)
	if t.read2PLOp.replied {
		logrus.Debugf("txn %v already reply read result to client %v", t.txnId, t.clientId)
		return
	}
	t.read2PLOp.replied = true
	if t.Status == ABORTED {
		t.read2PLOp.abort = true
	} else {
		t.read2PLOp.readResult = t.server.kvStore.read(t.readKeys)
	}
	t.read2PLOp.waitChan <- true
}

func (t *transaction) leaderPrepare() {
	logrus.Debugf("txn %v pp %v", t.txnId, t.participantPartition)
	if len(t.participantPartition) == 1 {
		// single partition; send the result to client;
		// do not require 2PC
		logrus.Debugf("single partition txn %v status %v", t.txnId, t.Status)
		if t.Status == PREPARED {
			t.Status = COMMITTED
		}
		if t.commitOp != nil {
			logrus.Debugf("single partition txn %v reply commit op", t.txnId)
			t.commitOp.result = t.Status == COMMITTED
			t.commitOp.waitChan <- true
		}
		// release lock
		t.partitionLeaderCommit()
		return
	}
	// send to coord
	go t.server.sendPrepare(t)
}

func (t *transaction) followerPrepare() {
	//t.status = PREPARED

}

func (t *transaction) partitionLeaderCommit() {
	logrus.Debugf("txn %v partition leader commit, status %v", t.txnId, t.Status)
	// write to kv store
	if t.Status == COMMITTED {
		t.server.kvStore.write(t.writeKeys, t.timestamp, t.clientId)
	}
	for key := range t.keyMap {
		t.server.lm.lockRelease(t, key)
	}
}

func (t *transaction) partitionFollowerCommit() {
	if t.Status == COMMITTED {
		t.server.kvStore.write(t.writeKeys, t.timestamp, t.clientId)
	}
}

type ReplicateMsgType int32

const (
	PREPARE ReplicateMsgType = iota
	COORDCOMMIT
	PARTITIONCOMMIT
)

type ReplicateMessage struct {
	TxnId       string
	Timestamp   int64
	ClientId    int64
	Status      TxnStatus
	WriteKeyVal map[string]string
	MsgType     ReplicateMsgType
	Priority    bool
}

func (t *transaction) replicate(status TxnStatus, msgType ReplicateMsgType) {
	logrus.Debugf("txn %v replicate status %v, msg type %v", t.txnId, status, msgType)
	msg := &ReplicateMessage{
		TxnId:       t.txnId,
		Timestamp:   t.timestamp,
		ClientId:    t.clientId,
		Status:      status,
		WriteKeyVal: nil,
		MsgType:     msgType,
		Priority:    t.priority,
	}
	if msgType == PREPARE {
		t.replicatedPrepare = true
	}
	if status == PREPARED {
		msg.WriteKeyVal = t.writeKeys
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
		logrus.Fatalf("replication encoding error: %v", err)
	}

	t.server.raft.raftInputChannel <- string(buf.Bytes())
}

// partition leader abort txn and replicate the result
func (t *transaction) abort() {
	logrus.Debugf("txn %v abort", t.txnId)
	if t.Status == READ {
		// if txn is in the read phase,
		// do not replicate
		t.Status = ABORTED
		t.replyRead()
		// release lock
		t.partitionLeaderCommit()
		return
	}
	t.Status = ABORTED
	// this is prepare message
	// after replication send to coord
	if !t.replicatedPrepare && !t.finalize {
		t.replicate(ABORTED, PREPARE)
	}
}

func (t *transaction) prepare() {
	logrus.Debugf("txn %v prepare", t.txnId)
	t.Status = PREPARED
	t.replicate(PREPARED, PREPARE)
}
