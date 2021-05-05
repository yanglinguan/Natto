package main

import (
	"Carousel-GTS/rpc"
	"golang.org/x/net/context"
)

func (c *NetworkMeasure) PredictLatency(ctx context.Context, request *rpc.LatencyRequest) (*rpc.LatencyReply, error) {
	delays := c.estimateArrivalTime(request.Per)
	return &rpc.LatencyReply{
		Delays: delays,
	}, nil
}
