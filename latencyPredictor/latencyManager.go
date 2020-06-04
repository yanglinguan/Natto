package latencyPredictor

import (
	"Carousel-GTS/utils"
	logger "github.com/sirupsen/logrus"
	"time"
)

type LatencyInfo struct {
	time    int64 // real time in ns
	latency int64 // ms
}

// Not thread safe.
type LatencyManager struct {
	// Uses up to the max number of latencies in the past windowLen.
	// When windowSize is 0, uses all of the latencies in the window.
	// When windowLen is 0, uses the min number of latencies in the past.
	// When both windowSize and windowLen is 0, uses the last latency
	wdwLen       int64 // window time length in ns
	wdwSize      int   // The min number of latencies for the window
	wdwBuf       *utils.Queue
	isWdwUpdated bool  // A flag to indicate whether the window has been updated
	wdw95th      int64 // in ms

	// Uses histogram instead of concrete latency numbers to track the latencies.
	// The histogram will be in ms-level from 1ms to 1s.
	// For now, considers any latency that is over 1s to be 1s
	// In a histogram array, each element is the number of the probing latencies in ms
	allHist          []int // All of the probing latencies since the beginning.
	all95th          int   //The 95th  latency (in ms) of all of the probing latencies
	isAll95thUpdated bool  // true if need to recalculate the 95th latency
}

func NewLatencyManager(windowLen time.Duration, windowSize int) *LatencyManager {
	lm := &LatencyManager{
		wdwLen:           windowLen.Nanoseconds(),
		wdwSize:          windowSize,
		wdwBuf:           utils.NewQueue(),
		isWdwUpdated:     false,
		wdw95th:          0,
		allHist:          make([]int, 1001, 1001), // index 1000 to be 1000ms
		all95th:          0,
		isAll95thUpdated: false,
	}
	return lm
}

// Reads a time duration in ms
func (lm *LatencyManager) toMs(t time.Duration) int64 {
	if t <= 0 {
		return t.Milliseconds()
	}
	return t.Milliseconds() + 1
}

// Accepts latency in ns
func (lm *LatencyManager) AddLat(lat time.Duration) {
	l := lm.toMs(lat)
	lm.updateWindow(l)
	//lm.updateAll(int(l)) // TODO Adds a switch to enable prediction based on all measurements
}

// updates window
func (lm *LatencyManager) updateWindow(t int64) {
	now := time.Now().UnixNano()
	li := &LatencyInfo{
		time:    now,
		latency: t,
	}
	// updates window
	lm.wdwBuf.Push(li)
	for e, ok := lm.wdwBuf.Peek(); ok && lm.wdwBuf.Size() > lm.wdwSize; lm.wdwBuf.Pop() {
		l := e.(*LatencyInfo)
		if l.time+lm.wdwLen >= now {
			break
		}
	}
	lm.isWdwUpdated = true
}

// Returns the 95th latency (in ms) of the current window
func (lm *LatencyManager) GetWindow95th() int64 {
	if lm.isWdwUpdated {
		lm.wdw95th = lm.GetWindowP(0.95)
		lm.isWdwUpdated = false
	}
	return lm.wdw95th
}

// Returns the specific percentile latency (in ms) of the current window
func (lm *LatencyManager) GetWindowP(p float64) int64 {
	if p > 1.0 {
		logger.Fatalf("Invalid percentile %f", p)
	}

	if lm.wdwBuf.Size() == 0 {
		logger.Fatalf("Latency probing window size is 0! Cannot calculate %f percentile", p)
	}

	l := make([]int64, lm.wdwBuf.Size())
	itr := lm.wdwBuf.Iterator()
	for i := 0; itr.HasNext(); i++ {
		n, _ := itr.Next()
		l[i] = n.(*LatencyInfo).latency
	}
	utils.QuickSort64n(l, 0, len(l)-1)

	//logger.Debugf("Window: %v", l)

	// Always takes the flooring of an float to calculate the 95th%
	if int(p) == 1 {
		return l[len(l)-1]
	}
	i := int(float64(len(l)) * p)
	/*
		// An alternative way to do the 95th% by using the round number of an float
		i := int(math.Round(float64(len(l))*p)) - 1
		if i < 0 {
			i = 0
		}
	*/
	return l[i]
}

//// Updates all hist
func (lm *LatencyManager) updateAll(t int) {
	if t >= len(lm.allHist) {
		t = len(lm.allHist) - 1
	}
	if t < 0 {
		t = 0 // TODO Supports negative value for timeOffset prediction
	}
	lm.allHist[t]++
	if t != lm.all95th {
		lm.isAll95thUpdated = true
	}
}

// Returns the 95th latency (in ms) from the beginning
func (lm *LatencyManager) GetAll95th() int {
	logger.Fatalf("TODO enable updateAll() function")
	if lm.isAll95thUpdated {
		lm.all95th = lm.GetAllPth(0.95)
		lm.isAll95thUpdated = false
	}
	return lm.all95th
}

// Returns the specific percentile latency in ms
func (lm *LatencyManager) GetAllPth(p float64) int {
	logger.Fatalf("TODO enable updateAll() function")
	return utils.CalHistPctl(lm.allHist, p)
}
