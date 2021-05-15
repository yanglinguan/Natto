package server

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/connection"
	"Carousel-GTS/raftnode"
	"Carousel-GTS/rpc"
	"net"
	"strings"

	"github.com/coreos/etcd/raft/raftpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	gRPCServer *grpc.Server
	config     configuration.Configuration

	storage *Storage // interface of different store, occ or gts (global timestamp)

	// schedule the txn by timestamp order
	scheduler Scheduler

	commitScheduler *CommitScheduler

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

	readResultFromCoordinatorChan chan *readResultFromCoordinatorRequest
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

	server.readResultFromCoordinatorChan = make(chan *readResultFromCoordinatorRequest, server.config.GetTotalClient())
	//server.executor = NewExecutor(server)

	switch server.config.GetServerMode() {
	case configuration.OCC:
		log.Debugf("server mode occ")
		server.operationCreator = NewOCCOperationCreator(server)
		break
	case configuration.PRIORITY:
		log.Debugf("server mode Timestamp global timestamp")
		server.operationCreator = NewPriorityOperationCreator(server)
		break
	case configuration.TwoPL:
		log.Debugf("server mode 2PL")
		server.operationCreator = NewTwoPLOperationCreator(server)
		break
	case configuration.TO:
		log.Debugf("server mode Timestamp ordering")
		server.operationCreator = NewTOOperationCreator(server)
		break
	default:
		log.Fatal("server mode should be either OCC or GTS")
		break
	}

	server.connections = make([]connection.Connection, len(server.config.GetServerAddress()))
	poolSize := server.config.GetConnectionPoolSize()
	if poolSize == 0 {
		for sId, addr := range server.config.GetServerAddress() {
			if sId == serverId {
				continue
			}
			log.Debugf("add connection server %v, addr %v", sId, addr)
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

	server.coordinator = NewCoordinator(server)
	server.storage = NewStorage(server)
	if server.config.UseNetworkTimestamp() {
		server.scheduler = NewTimestampScheduler(server)
	} else if server.config.GetServerMode() == configuration.PRIORITY {
		if server.config.UsePriorityScheduler() {
			server.scheduler = NewPriorityScheduler(server)
		} else {
			server.scheduler = NewNoScheduler(server)
		}
	} else {
		server.scheduler = NewNoScheduler(server)
	}

	server.commitScheduler = NewCommitScheduler(server)
	//server.storage.LoadKeys(server.config.GetKeyListByPartitionId(server.partitionId))
	keyList := server.config.GetKeyListByPartitionId(server.partitionId)
	if server.config.GetWorkLoad() == configuration.SMALLBANK {
		// Small Bank Workload
		server.storage.InitSmallBankData(
			keyList,
			server.config.GetSbCheckingAccountFlag(),
			server.config.GetSbSavingsAccountFlag(),
			server.config.GetSbInitBalance(),
			server.config.GetSbInitBalance(),
		)
	} else {
		server.storage.LoadKeys(keyList)
	}

	rpc.RegisterCarouselServer(server.gRPCServer, server)
	reflection.Register(server.gRPCServer)

	log.Debugf("connection %v", server.connections)
	return server
}

type readResultFromCoordinatorRequest struct {
	clientId  string
	wait      chan bool
	replyChan chan *rpc.ReadReplyFromCoordinator
}

func NewReadResultFromCoordinatorRequest(clientId string) *readResultFromCoordinatorRequest {
	return &readResultFromCoordinatorRequest{
		clientId: clientId,
		wait:     make(chan bool),
	}
}

func (req *readResultFromCoordinatorRequest) block() bool {
	return <-req.wait
}

func (server *Server) handleReadResultFromCoordinatorRequest() {
	for {
		request := <-server.readResultFromCoordinatorChan
		if _, exist := server.coordinator.clientReadRequestChan[request.clientId]; !exist {
			server.coordinator.clientReadRequestChan[request.clientId] = make(chan *rpc.ReadReplyFromCoordinator, server.config.GetQueueLen())
		}
		request.replyChan = server.coordinator.clientReadRequestChan[request.clientId]
		request.wait <- true
	}
}

func (server *Server) Start() {
	log.Infof("Starting Server %v", server.serverId)
	go server.handleReadResultFromCoordinatorRequest()
	if server.config.GetReplication() {
		// The channel for proposing operations to Raft
		raftInputChannel := make(chan string, server.config.GetQueueLen())
		defer close(raftInputChannel)
		raftConfChangeChannel := make(chan raftpb.ConfChange)
		defer close(raftConfChangeChannel)
		isLeader := server.serverId == server.config.GetExpectPartitionLeaders()[server.partitionId]
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
			isLeader,
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
