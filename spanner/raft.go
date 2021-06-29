package spanner

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/coreos/etcd/snap"
	"github.com/sirupsen/logrus"
)

type raft struct {
	server *Server

	raftInputChannel  chan<- string
	raftOutputChannel <-chan *string
	raftErrorChannel  <-chan error
	// TODO: snapshot
	raftSnapshotter *snap.Snapshotter
}

func newRaft(server *Server, snapshotter *snap.Snapshotter,
	raftInputChannel chan<- string,
	raftOutputChannel <-chan *string,
	raftErrorChannel <-chan error) *raft {
	r := &raft{
		server:            server,
		raftInputChannel:  raftInputChannel,
		raftOutputChannel: raftOutputChannel,
		raftErrorChannel:  raftErrorChannel,
		raftSnapshotter:   snapshotter,
	}
	go r.run()
	return r
}

func (r *raft) run() {
	for {
		data := <-r.raftOutputChannel
		if data == nil {
			logrus.Warn("get empty data")
			continue
		}

		go r.handleReplicatedOp(data)
	}
}

type replicateResultOp struct {
	replicationMsg ReplicateMessage
}

func (o *replicateResultOp) wait() {
	return
}

func (o *replicateResultOp) string() string {
	return fmt.Sprintf("REPLICATION RESULT OP txn %v msgType %v status %v",
		o.replicationMsg.TxnId, o.replicationMsg.MsgType, o.replicationMsg.Status)
}

func (o *replicateResultOp) execute(s *Server) {
	switch o.replicationMsg.MsgType {
	case PREPARE:
		s.applyPrepare(o.replicationMsg)
	case PARTITIONCOMMIT:
		s.applyPartitionCommit(o.replicationMsg)
	default:
		return
	}
}

type replicatedCoordCommitResult struct {
	replicationMsg ReplicateMessage
}

func (o *replicatedCoordCommitResult) wait() {
	return
}

func (o *replicatedCoordCommitResult) string() string {
	return fmt.Sprintf("REPLICATION RESULT OP txn %v msgType %v status %v",
		o.replicationMsg.TxnId, o.replicationMsg.MsgType, o.replicationMsg.Status)
}

func (o *replicatedCoordCommitResult) execute(coord *coordinator) {
	coord.applyCoordCommit(o.replicationMsg)
}

func (r *raft) handleReplicatedOp(data *string) {
	decoder := gob.NewDecoder(bytes.NewBufferString(*data))
	var replicationMsg ReplicateMessage
	if err := decoder.Decode(&replicationMsg); err != nil {
		logrus.Fatalf("Decoding error %v", err)
	}
	logrus.Debugf("get replicated msg txn %v status %v",
		replicationMsg.TxnId, replicationMsg.Status)

	if replicationMsg.MsgType == COORDCOMMIT {
		op := &replicatedCoordCommitResult{replicationMsg: replicationMsg}
		r.server.coordinator.addOp(op)
	} else {
		op := &replicateResultOp{replicationMsg: replicationMsg}
		r.server.addOp(op)
	}
}
