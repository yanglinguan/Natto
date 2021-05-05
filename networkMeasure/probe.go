package main

import (
	"Carousel-GTS/latencyPredictor"
	"sync"
	"time"
)

type LatInfo struct {
	addr   string        // replica network address
	rt     time.Duration // roundtrip time including queuing delay
	qDelay time.Duration // queuing delay (in ns) on the replica
}

type LatTimeInfo struct {
	addr       string        // replica network address
	rt         time.Duration // roundtrip time including queuing delay
	timeOffset time.Duration // time offset between the clock time of sending (on client) and processing (on server)
}

func (c *NetworkMeasure) processProbe() {
	for {
		latInfo := <-c.probeC
		oneWayLat := (latInfo.rt + latInfo.qDelay) / 2
		c.latencyPredictor.AddProbeRet(&latencyPredictor.ProbeRet{
			Addr: latInfo.addr,
			Rt:   oneWayLat,
		})
	}
}

func (c *NetworkMeasure) processProbeTime() {
	for {
		latTimeInfo := <-c.probeTimeC
		c.latencyPredictor.AddProbeRet(&latencyPredictor.ProbeRet{
			Addr: latTimeInfo.addr,
			Rt:   latTimeInfo.timeOffset,
		})
	}
}

func (c *NetworkMeasure) probing() {
	probeTimer := time.NewTimer(c.Config.GetProbeInterval())
	for {
		<-probeTimer.C
		c.probe()
		probeTimer.Reset(c.Config.GetProbeInterval())
	}
}

func (c *NetworkMeasure) probingTime() {
	probeTimer := time.NewTimer(c.Config.GetProbeInterval())
	for {
		<-probeTimer.C
		c.probeTime()
		probeTimer.Reset(c.Config.GetProbeInterval())
	}
}

func (c *NetworkMeasure) probe() {
	var wg sync.WaitGroup
	for sId := range c.connections {
		wg.Add(1)
		go func(sId int) {
			sender := NewProbeSender(sId, c)
			start := time.Now()
			queueingDelay := sender.Send()
			rt := time.Since(start)
			c.probeC <- &LatInfo{
				addr:   c.Config.GetServerAddressByServerId(sId),
				rt:     rt,
				qDelay: time.Duration(queueingDelay),
			}
			wg.Done()
		}(sId)
	}

	if c.Config.IsProbeBlocking() {
		wg.Wait()
	}
}

func (c *NetworkMeasure) probeTime() {
	var wg sync.WaitGroup
	for sId := range c.connections {
		wg.Add(1)
		go func(sId int) {
			sender := NewProbeTimeSender(sId, c)
			start := time.Now()
			pTime := sender.Send()
			rt := time.Since(start)
			c.probeTimeC <- &LatTimeInfo{
				addr:       c.Config.GetServerAddressByServerId(sId),
				rt:         rt,
				timeOffset: time.Duration(pTime - start.UnixNano()),
			}
			wg.Done()
		}(sId)
	}

	if c.Config.IsProbeBlocking() {
		wg.Wait()
	}
}

func (c *NetworkMeasure) estimateArrivalTime(per int32) []int64 {
	result := make([]int64, c.Config.GetServerNum())
	for sId := 0; sId < c.Config.GetServerNum(); sId++ {
		addr := c.Config.GetServerAddressByServerId(sId)
		lat := c.latencyPredictor.PredictLat(addr, int(per))
		result[sId] = lat
	}

	return result
}
