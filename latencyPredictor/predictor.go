package latencyPredictor

import (
	logger "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type ProbeRet struct {
	Addr string
	Rt   time.Duration // round-trip time
}

// Thread safe latency manager
type syncLatencyManager struct {
	lm   *LatencyManager
	lock sync.Mutex
}

func (l *syncLatencyManager) AddProbeRet(pr *ProbeRet) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.lm.AddLat(pr.Rt)
}

func (l *syncLatencyManager) GetWindow95th() int64 {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.lm.GetWindow95th()
}

func (l *syncLatencyManager) GetWindow99th() int64 {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.lm.GetWindow99th()
}

type LatencyPredictor struct {
	dstTable map[string]*syncLatencyManager
}

func NewLatencyPredictor(dstList []string, windowLen time.Duration, windowSize int) *LatencyPredictor {
	p := &LatencyPredictor{
		dstTable: make(map[string]*syncLatencyManager),
	}
	for _, dst := range dstList {
		p.dstTable[dst] = &syncLatencyManager{
			lm: NewLatencyManager(windowLen, windowSize),
		}
	}
	return p
}

func (pm *LatencyPredictor) getLatMgr(addr string) *syncLatencyManager {
	l, ok := pm.dstTable[addr]
	if !ok {
		logger.Fatalf("There is no latency manager for addr = %s", addr)
	}
	return l
}

func (pm *LatencyPredictor) AddProbeRet(pr *ProbeRet) {
	l := pm.getLatMgr(pr.Addr)
	l.AddProbeRet(pr)
}

// Returns the predicted roundtrip latency in ms
func (pm *LatencyPredictor) PredictLat(addr string, per int) int64 {
	l := pm.getLatMgr(addr)
	if per == 95 {
		return l.GetWindow95th()
	}
	if per == 99 {
		return l.GetWindow99th()
	}

	return 0
}
