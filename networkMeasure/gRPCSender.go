package main

import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type ProbeSender struct {
	dstServerId int
	client      *NetworkMeasure
}

func NewProbeSender(dstServerId int, client *NetworkMeasure) *ProbeSender {
	s := &ProbeSender{
		dstServerId: dstServerId,
		client:      client,
	}
	return s
}

func (p *ProbeSender) Send() int64 {
	conn := p.client.connections[p.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	reply, err := client.Probe(context.Background(), &rpc.ProbeReq{
		FromCoordinator: false,
	})
	if err != nil {
		logrus.Fatalf("cannot sent probe request to server %v: %v", conn.GetDstAddr(), err)
		return -1
	}
	return reply.QueuingDelay
}

type ProbeTimeSender struct {
	dstServerId int
	client      *NetworkMeasure
}

func NewProbeTimeSender(dstServerId int, client *NetworkMeasure) *ProbeTimeSender {
	s := &ProbeTimeSender{
		dstServerId: dstServerId,
		client:      client,
	}
	return s
}

func (p *ProbeTimeSender) Send() int64 {
	conn := p.client.connections[p.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	reply, err := client.ProbeTime(context.Background(), &rpc.ProbeReq{
		FromCoordinator: false,
	})
	if err != nil {
		logrus.Fatalf("cannot sent probeTime request to server %v: %v", conn.GetDstAddr(), err)
		return -1
	}
	return reply.ProcessTime
}
