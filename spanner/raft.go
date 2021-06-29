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
	return fmt.Sprintf("REPLICATION RESULT OP txn %v", o.replicationMsg.TxnId)
}

func (o *replicateResultOp) execute(s *Server) {
	switch o.replicationMsg.MsgType {
	case PREPARE:
		s.applyPrepare(o.replicationMsg)
	case COORDCOMMIT:
		s.applyCoordCommit(o.replicationMsg)
	case PARTITIONCOMMIT:
		s.applyPartitionCommit(o.replicationMsg)
	default:
		return
	}
}

func (r *raft) handleReplicatedOp(data *string) {
	decoder := gob.NewDecoder(bytes.NewBufferString(*data))
	var replicationMsg ReplicateMessage
	if err := decoder.Decode(&replicationMsg); err != nil {
		logrus.Fatalf("Decoding error %v", err)
	}
	logrus.Debugf("get replicated msg txn %v status %v",
		replicationMsg.TxnId, replicationMsg.Status)
	op := &replicateResultOp{replicationMsg: replicationMsg}
	r.server.addOp(op)
}
