package server

import (
	"bytes"
	"encoding/gob"
	"github.com/coreos/etcd/snap"
	"github.com/sirupsen/logrus"
)

type Raft struct {
	server *Server

	raftInputChannel  chan<- string
	raftOutputChannel <-chan *string
	raftErrorChannel  <-chan error
	// TODO: snapshot
	raftSnapshotter *snap.Snapshotter
}

func NewRaft(server *Server, snapshotter *snap.Snapshotter,
	raftInputChannel chan<- string,
	raftOutputChannel <-chan *string,
	raftErrorChannel <-chan error) *Raft {
	r := &Raft{
		server:            server,
		raftInputChannel:  raftInputChannel,
		raftOutputChannel: raftOutputChannel,
		raftErrorChannel:  raftErrorChannel,
		raftSnapshotter:   snapshotter,
	}

	return r
}

func (r *Raft) run() {
	for {
		data := <-r.raftOutputChannel
		if data == nil {
			continue
		}

		r.handleReplicatedOp(data)
	}
}

func (r *Raft) handleReplicatedOp(data *string) {
	decoder := gob.NewDecoder(bytes.NewBufferString(*data))
	var replicationMsg ReplicationMsg
	if err := decoder.Decode(&replicationMsg); err != nil {
		logrus.Errorf("Decoding error %v", err)
	}
	if replicationMsg.isFromCoordinator {
		r.server.coordinator.Replication <- replicationMsg
	} else {
		r.server.executor.ReplicationTxn <- replicationMsg
	}
}