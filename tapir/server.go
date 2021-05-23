package tapir

import (
	"Carousel-GTS/configuration"
	"net"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// TAPIR server instance
type Server struct {
	UnimplementedIrRpcServer

	// Network I/O
	grpcServer *grpc.Server

	ir *IrReplica
}

func NewServer(
	sId string,
	chSize int,
	isTs bool, // If uses timestamp-based ordering
	clockSkew int64,
) *Server {
	t := NewTapirStore(sId, NewVersionStore())
	s := &Server{
		ir: NewIrReplica(
			sId,
			t,
			chSize,
			clockSkew,
		),
	}
	return s
}

func (s *Server) InitData(keyList []string, val string, config configuration.Configuration) {
	if config.GetWorkLoad() == configuration.SMALLBANK {
		s.ir.GetApp().(*TapirStore).InitSmallBankData(keyList,
			config.GetSbCheckingAccountFlag(),
			config.GetSbSavingsAccountFlag(),
			config.GetSbInitBalance(),
			config.GetSbInitBalance())
	} else {
		s.ir.GetApp().(*TapirStore).InitData(keyList, val)
	}
}

func (s *Server) Start(port string) {
	// Start IR / TAPIR server
	s.ir.Start()

	// Start rpc server
	s.startGRpc(port)
}

func (s *Server) startGRpc(port string) {
	// Creates the gRPC server instance
	s.grpcServer = grpc.NewServer()
	RegisterIrRpcServer(s.grpcServer, s)
	reflection.Register(s.grpcServer)

	// Starts RPC service
	rpcListener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Errorf("Fails to listen on port %s \nError: %v", port, err)
	}

	logger.Infof("Starting gRPC services")

	err = s.grpcServer.Serve(rpcListener)
}
