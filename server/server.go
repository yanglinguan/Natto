package server

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/connection"
	"Carousel-GTS/rpc"
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

	connections   map[string]connection.Connection
	serverAddress string
	partitionId   int
	serverId      string
	port          string
}

func NewServer(serverId string, configFile string) *Server {
	server := &Server{
		serverId:    serverId,
		gRPCServer:  grpc.NewServer(),
		connections: make(map[string]connection.Connection),
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
		server.scheduler = NewTimestampScheduler(server)
		server.storage = NewGTSStorageWithReorder(server)
	case configuration.OCCReadOnly:
		server.scheduler = &NoScheduler{server: server}
		server.storage = NewOccStorageWithReadOnly(server)
		break
	default:
		log.Fatal("OCC, GTS, GTS_DEP_GRAPH")
		break
	}

	server.storage.LoadKeys(server.config.GetKeyListByPartitionId(server.partitionId))
	poolSize := server.config.GetConnectionPoolSize()
	if poolSize == 0 {
		for sId, addr := range server.config.GetServerAddressMap() {
			server.connections[sId] = connection.NewSingleConnect(addr)
		}
	} else {
		for sId, addr := range server.config.GetServerAddressMap() {
			server.connections[sId] = connection.NewPoolConnection(addr, poolSize)
		}
	}

	rpc.RegisterCarouselServer(server.gRPCServer, server)
	reflection.Register(server.gRPCServer)

	return server
}

func (s *Server) Start() {
	log.Infof("Starting Server %v", s.serverId)

	// Starts RPC service
	rpcListener, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		log.Fatalf("Fails to listen on port %s \nError: %v", s.port, err)
	}

	err = s.gRPCServer.Serve(rpcListener)
	if err != nil {
		log.Fatalf("Cannot start RPC services. \nError: %v", err)
	}
}
