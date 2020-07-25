package server

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/connection"
	"Carousel-GTS/raftnode"
	"Carousel-GTS/rpc"
	"github.com/coreos/etcd/raft/raftpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"strings"
)

type Server struct {
	gRPCServer *grpc.Server
	config     configuration.Configuration

	storage *Storage // interface of different store, occ or gts (global timestamp)

	// schedule the txn by timestamp order
	scheduler *Scheduler

	// executor execute the txn when it is committed or abort
	//executor        *Executor

	coordinator *Coordinator

	operationCreator OperationCreator

	raft *Raft

	raftNode *raftnode.RaftNode

	connections   []connection.Connection
	serverAddress string
	partitionId   int
	serverId      int
	port          string

	getLeaderId func() uint64
}

func NewServer(serverId int, configFile string) *Server {
	server := &Server{
		serverId:   serverId,
		gRPCServer: grpc.NewServer(),
	}

	server.config = configuration.NewFileConfiguration(configFile)
	server.serverAddress = server.config.GetServerAddressByServerId(server.serverId)
	server.port = strings.Split(server.serverAddress, ":")[1]
	server.partitionId = server.config.GetPartitionIdByServerId(server.serverId)

	//server.executor = NewExecutor(server)
	server.coordinator = NewCoordinator(server)
	server.storage = NewStorage(server)
	server.scheduler = NewScheduler(server)

	switch server.config.GetServerMode() {
	case configuration.OCC:
		log.Debugf("server mode occ")
		server.operationCreator = NewOCCOperationCreator(server)
		//server.storage = NewOccStorage(server)
		break
	case configuration.PRIORITY:
		log.Debugf("server mode Timestamp global timestamp")
		server.operationCreator = NewPriorityOperationCreator(server)
		//server.scheduler = NewTimestampScheduler(server)
		//server.storage = NewGTSStorage(server)
		break
	case configuration.TwoPL:
		log.Debugf("server mode 2PL")
		server.operationCreator = NewTwoPLOperationCreator(server)
	case configuration.TO:
		log.Debugf("server mode Timestamp ordering")
		server.operationCreator = NewTOOperationCreator(server)
	default:
		log.Fatal("server mode should be either OCC or GTS")
		break
	}

	server.storage.LoadKeys(server.config.GetKeyListByPartitionId(server.partitionId))
	server.connections = make([]connection.Connection, len(server.config.GetServerAddress()))
	poolSize := server.config.GetConnectionPoolSize()
	if poolSize == 0 {
		for sId, addr := range server.config.GetServerAddress() {
			if sId == serverId {
				continue
			}
			server.connections[sId] = connection.NewSingleConnect(addr)
		}
	} else {
		for sId, addr := range server.config.GetServerAddress() {
			if sId == serverId {
				continue
			}
			server.connections[sId] = connection.NewPoolConnection(addr, poolSize)
		}
	}

	rpc.RegisterCarouselServer(server.gRPCServer, server)
	reflection.Register(server.gRPCServer)

	return server
}

func (server *Server) Start() {
	log.Infof("Starting Server %v", server.serverId)

	if server.config.GetReplication() {
		// The channel for proposing operations to Raft
		raftInputChannel := make(chan string, server.config.GetQueueLen())
		defer close(raftInputChannel)
		raftConfChangeChannel := make(chan raftpb.ConfChange)
		defer close(raftConfChangeChannel)

		// TODO: snapshot function
		getSnapshotFunc := func() ([]byte, error) { return make([]byte, 0), nil }
		raftOutputChannel, raftErrorChannel, raftSnapshotterChannel, getLeaderIdFunc, raftNode := raftnode.NewRaftNode(
			server.config.GetRaftIdByServerId(server.serverId)+1,
			server.config.GetRaftPortByServerId(server.serverId),
			server.config.GetRaftPeersByServerId(server.serverId),
			false,
			getSnapshotFunc,
			raftInputChannel,
			raftConfChangeChannel,
			server.config.GetQueueLen(),
		)

		server.getLeaderId = getLeaderIdFunc
		server.raftNode = raftNode

		server.raft = NewRaft(server, <-raftSnapshotterChannel, raftInputChannel, raftOutputChannel, raftErrorChannel)
	}

	// Starts RPC service
	rpcListener, err := net.Listen("tcp", ":"+server.port)
	if err != nil {
		log.Fatalf("Fails to listen on port %s \nError: %v", server.port, err)
	}

	err = server.gRPCServer.Serve(rpcListener)
	if err != nil {
		log.Fatalf("Cannot start RPC services. \nError: %v", err)
	}
}

func (server *Server) GetLeaderServerId() int {
	// Member id is the index of the network address in the RpcPeerList
	if !server.config.GetReplication() {
		return server.serverId
	}

	id := server.getLeaderId()
	if id == 0 {
		return -1
	}
	leaderAddr := server.config.GetServerIdByRaftId(int(id)-1, server.serverId)
	return leaderAddr
}

func (server *Server) IsLeader() bool {
	if !server.config.GetReplication() {
		return true
	}
	leaderId := server.GetLeaderServerId()
	return leaderId == server.serverId
}

func (server *Server) StartOp(op ReadAndPrepareOp) {
	if server.config.UseNetworkTimestamp() || server.config.GetServerMode() == configuration.PRIORITY {
		log.Debugf("txn %v add to scheduler")
		server.scheduler.AddOperation(op)
	} else {
		log.Debugf("txn %v add to storage")
		server.storage.AddOperation(op)
	}
}
