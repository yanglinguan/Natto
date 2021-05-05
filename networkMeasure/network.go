package main

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/connection"
	"Carousel-GTS/latencyPredictor"
	"Carousel-GTS/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"strings"
)

type NetworkMeasure struct {
	Config configuration.Configuration

	latencyPredictor *latencyPredictor.LatencyPredictor
	probeC           chan *LatInfo
	probeTimeC       chan *LatTimeInfo

	connections []connection.Connection

	gRPCServer *grpc.Server
	port       string
}

func NewNetworkMeasure(dcId int, configFile string) *NetworkMeasure {
	config := configuration.NewFileConfiguration(configFile)
	queueLen := config.GetQueueLen()
	c := &NetworkMeasure{
		Config:      config,
		connections: make([]connection.Connection, len(config.GetServerAddress())),
	}

	if c.Config.IsDynamicLatency() && c.Config.UseNetworkTimestamp() {
		if c.Config.GetConnectionPoolSize() == 0 {
			for sId, addr := range c.Config.GetServerAddress() {
				c.connections[sId] = connection.NewSingleConnect(addr)
			}
		} else {
			for sId, addr := range c.Config.GetServerAddress() {
				c.connections[sId] = connection.NewPoolConnection(addr, c.Config.GetConnectionPoolSize())
			}
		}
		c.latencyPredictor = latencyPredictor.NewLatencyPredictor(
			c.Config.GetServerAddress(),
			c.Config.GetProbeWindowLen(),
			c.Config.GetProbeWindowMinSize())
		if c.Config.IsProbeTime() {
			c.probeTimeC = make(chan *LatTimeInfo, queueLen)
		} else {
			c.probeC = make(chan *LatInfo, queueLen)
		}

		if c.Config.IsProbeTime() {
			go c.probingTime()
			go c.processProbeTime()
		} else {
			go c.probing()
			go c.processProbe()
		}
	}
	addr := config.GetNetworkMeasureAddr(dcId)
	c.port = strings.Split(addr, ":")[1]
	rpc.RegisterNetworkMeasureServer(c.gRPCServer, c)
	reflection.Register(c.gRPCServer)

	return c
}

func (c *NetworkMeasure) Start() {
	// Starts RPC service
	rpcListener, err := net.Listen("tcp", ":"+c.port)
	if err != nil {
		log.Fatalf("Fails to listen on port %s \nError: %v", c.port, err)
	}

	err = c.gRPCServer.Serve(rpcListener)
	if err != nil {
		log.Fatalf("Cannot start RPC services. \nError: %v", err)
	}
}
