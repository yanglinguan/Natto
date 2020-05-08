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
	gRPCServer  *grpc.Server
	config      configuration.Configuration
	scheduler   Scheduler
	storage     Storage
	executor    *Executor
	coordinator *Coordinator
	raft        *Raft

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

	server.executor = NewExecutor(server)
	server.coordinator = NewCoordinator(server)

	switch server.config.GetServerMode() {
	case configuration.OCC:
		server.scheduler = &NoScheduler{server: server}
		server.storage = NewOccStorage(server)
		break
	case configuration.GTS:
		server.scheduler = NewTimestampScheduler(server)
		server.storage = NewGTSStorage(server)
		break
	case configuration.GtsDepGraph:
		server.scheduler = NewTimestampScheduler(server)
		server.storage = NewGTSStorageDepGraph(server)
		break
	case configuration.GTSReorder:
		//server.scheduler = NewTimestampScheduler(server)
		//server.storage = NewGTSStorageWithReorder(server)
		break
	default:
		log.Fatal("OCC, GTS, GTS_DEP_GRAPH")
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
		// The channel for proposing requests to Raft
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
