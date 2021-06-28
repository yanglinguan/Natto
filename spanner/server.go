package spanner

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/raftnode"
	"context"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"strings"
)

type Server struct {
	serverId int
	pId      int // partition Id

	kvStore  *kvStore
	lm       *lockManager
	txnStore *txnStore

	coordinator *coordinator

	gRPCServer *grpc.Server

	raft *raft

	config configuration.Configuration

	getLeaderId func() uint64

	connection []*connection

	opChan chan operation

	opCreator opCreator

	port          string
	serverAddress string
}

func NewServer(serverId int, config configuration.Configuration) *Server {
	s := &Server{
		serverId:      serverId,
		pId:           config.GetPartitionIdByServerId(serverId),
		kvStore:       newKVStore(),
		lm:            newLockManager(),
		txnStore:      newTxnStore(),
		coordinator:   newCoordinator(config.GetQueueLen()),
		gRPCServer:    grpc.NewServer(),
		raft:          nil,
		config:        config,
		getLeaderId:   nil,
		connection:    make([]*connection, config.GetServerNum()),
		opChan:        make(chan operation, config.GetQueueLen()),
		opCreator:     &twoPLOpCreator{},
		serverAddress: config.GetServerAddressByServerId(serverId),
	}
	s.port = strings.Split(s.serverAddress, ":")[1]

	for sId, addr := range config.GetServerAddress() {
		if sId == serverId {
			continue
		}
		logrus.Debugf("add connection server %v, addr %v", sId, addr)
		s.connection[sId] = newConnect(addr)
	}

	RegisterSpannerServer(s.gRPCServer, s)
	reflection.Register(s.gRPCServer)

	return s
}

func (s *Server) InitKeyValue(key string, val string) {
	s.kvStore.initKeyValue(key, val)
}

func (s *Server) InitData(key []string) {
	s.kvStore.loadKeys(key)
}

func (s *Server) handleOp() {
	for {
		op := <-s.opChan
		op.execute(s)
	}
}

func (s *Server) Start() {
	logrus.Debugf("Starting Server %v", s.serverId)
	go s.handleOp()
	if s.config.GetReplication() {
		// The channel for proposing operations to Raft
		raftInputChannel := make(chan string, s.config.GetQueueLen())
		defer close(raftInputChannel)
		raftConfChangeChannel := make(chan raftpb.ConfChange)
		defer close(raftConfChangeChannel)
		isLeader := s.serverId == s.config.GetExpectPartitionLeaders()[s.pId]
		// TODO: snapshot function
		getSnapshotFunc := func() ([]byte, error) { return make([]byte, 0), nil }
		raftOutputChannel, raftErrorChannel, raftSnapshotterChannel, getLeaderIdFunc, _ := raftnode.NewRaftNode(
			s.config.GetRaftIdByServerId(s.serverId)+1,
			s.config.GetRaftPortByServerId(s.serverId),
			s.config.GetRaftPeersByServerId(s.serverId),
			false,
			getSnapshotFunc,
			raftInputChannel,
			raftConfChangeChannel,
			s.config.GetQueueLen(),
			isLeader,
		)

		s.getLeaderId = getLeaderIdFunc
		//s.raftNode = raftNode

		s.raft = newRaft(s, <-raftSnapshotterChannel, raftInputChannel, raftOutputChannel, raftErrorChannel)
	}

	// Starts RPC service
	rpcListener, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		log.Fatalf("Fails to listen on port %s \nError: %v", s.port, err)
	}
	logrus.Debugf("server %v listen on port %v", s.serverId, s.port)
	err = s.gRPCServer.Serve(rpcListener)
	if err != nil {
		log.Fatalf("Cannot start RPC services. \nError: %v", err)
	}
}

func (s *Server) applyPrepare(message ReplicateMessage) {
	logrus.Debugf("txn %v replicated prepare status %v", message.TxnId, message.Status)
	txn := s.txnStore.createTxn(message.TxnId, message.Timestamp, message.ClientId, s)
	txn.Status = message.Status
	if s.IsLeader() {
		txn.leaderPrepare()
	} else {
		txn.writeKeys = message.WriteKeyVal
		//txn.followerPrepare()
	}
}

func (s *Server) applyCoordCommit(message ReplicateMessage) {
	logrus.Debugf("txn %v replicated coord commit status %v", message.TxnId, message.Status)
	txn := s.txnStore.createTxn(message.TxnId, message.Timestamp, message.ClientId, s)
	txn.Status = message.Status
	if s.IsLeader() {
		txn.coordLeaderCommit()
	} else {
		txn.coordFollowerCommit()
	}
}

func (s *Server) applyPartitionCommit(message ReplicateMessage) {
	logrus.Debugf("txn %v replicated partition commit status %v", message.TxnId, message.Status)
	txn := s.txnStore.createTxn(message.TxnId, message.Timestamp, message.ClientId, s)
	txn.Status = message.Status
	if s.IsLeader() {
		txn.partitionLeaderCommit()
	} else {
		txn.partitionFollowerCommit()
	}
}

func (s *Server) GetLeaderServerId() int {
	// Member id is the index of the network address in the RpcPeerList
	if !s.config.GetReplication() {
		return s.serverId
	}

	id := s.getLeaderId()
	if id == 0 {
		return -1
	}
	leaderAddr := s.config.GetServerIdByRaftId(int(id)-1, s.serverId)
	return leaderAddr
}

func (s *Server) IsLeader() bool {
	if !s.config.GetReplication() {
		return true
	}
	leaderId := s.GetLeaderServerId()
	return leaderId == s.serverId
}

func (s *Server) sendPrepare(txn *transaction) {
	coordServerId := s.config.GetLeaderIdByPartitionId(txn.coordPId)
	prepareRequest := &PrepareRequest{
		Id:       txn.txnId,
		Prepared: txn.Status == PREPARED,
		PId:      int32(s.pId),
	}

	if coordServerId == s.serverId {
		p := newPrepare(prepareRequest, s)
		s.coordinator.opChan <- p
		return
	}
	// send to coord and wait for commit result
	conn := s.connection[coordServerId].GetConn()
	client := NewSpannerClient(conn)
	logrus.Debugf("txn %v send commit to coord server %v, pId %v", txn.txnId, coordServerId, txn.coordPId)
	_, err := client.Prepare(context.Background(), prepareRequest)
	if err != nil {
		logrus.Fatalf("txn %v cannot sent prepare result to coordinator %v",
			txn.txnId, coordServerId)
	}
}

func (s *Server) sendCommitDecision(txn *transaction, pId int) {
	twoPCInfo := s.coordinator.transactions[txn.txnId]

	commitResult := &CommitResult{
		Commit: twoPCInfo.commitOp.result,
		Id:     txn.txnId,
	}

	// send to participant partition
	leaderId := s.config.GetLeaderIdByPartitionId(pId)

	//if leaderId == s.serverId {
	//	op := &commitDecision{commitResult: commitResult}
	//	s.opChan <- op
	//	return
	//}

	logrus.Debugf("txn %v send commit decision to server %v pId %v", txn.txnId, leaderId, pId)
	conn := s.connection[leaderId]
	client := NewSpannerClient(conn.GetConn())

	_, err := client.CommitDecision(context.Background(), commitResult)
	if err != nil {
		logrus.Fatalf("txn %v coord cannot sent commit decision to partition %v",
			txn.txnId, pId)
	}
}
