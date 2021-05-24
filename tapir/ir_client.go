package tapir

import (
	"strconv"
	"sync"

	"github.com/op/go-logging"
)

type IrClientApp interface {
	// resultTable: string --> number
	Decide(resultTable map[string]int) string
}

type IrClient struct {
	app IrClientApp

	id       string
	reqCount int
	lock     sync.Mutex

	// Replica info
	replicaAddrList []string // a list of replica network addresses (ip:port)
	replicaN        int      // total number of replicas
	majority        int
	superMajority   int

	io IO             // Network I/O interface
	wg sync.WaitGroup // I/O operation wait group
}

func NewIrClient(
	app IrClientApp,
	id string,
	replicaAddrList []string,
	io IO,
) *IrClient {
	c := &IrClient{
		app:             app,
		id:              id,
		reqCount:        0,
		replicaAddrList: replicaAddrList,
		io:              io,
	}

	// Replica info
	c.replicaN = len(c.replicaAddrList)
	if c.replicaN < 3 || c.replicaN%2 == 0 {
		logger.Fatalf("IR: invalid number of replicas = %d ", c.replicaN)
	}
	// 2f+1 configuration
	f := c.replicaN >> 1
	c.majority = f + 1
	c.superMajority = (3*f)>>1 + f%2 + 1
	//logger.Debugf("Replica Info: replicaN = %d majority = %d supermajority = %d",
	//c.replicaN, c.majority, c.superMajority)

	return c
}

// Blocking I/O
func (c *IrClient) InovkeUnlog(addr string, op []byte) []byte {
	reply := c.io.ProposeUnLogOp(addr, op)
	return reply.Ret
}

// Blocking I/O
func (c *IrClient) InvokeInconsistent(op []byte) {
	reqId := c.genReqId()
	retC := make(chan bool, len(c.replicaAddrList))

	//logger.Debugf("IR starts inconsistent request id = %s", reqId)

	// Propose
	for _, addr := range c.replicaAddrList {
		c.wg.Add(1)
		go func(addr string) {
			defer c.wg.Done()
			//logger.Debugf("IR proposes inconsistent request id = %s to addr = %s", reqId, addr)
			c.io.ProposeIncOp(addr, reqId, op)
			retC <- true
		}(addr)
	}

	// Wait for responses from a majority of replicas
	replyCount := 0
	for _ = range retC {
		replyCount++
		if replyCount >= c.majority {
			break
		}
	}

	// Finalize (asynchronously)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for _, addr := range c.replicaAddrList {
			c.wg.Add(1)
			go func(addr string) {
				defer c.wg.Done()
				//logger.Debugf("IR finalizes inconsistent request id = %s to addr = %s", reqId, addr)
				c.io.FinalizeIncOp(addr, reqId)
			}(addr)
		}
	}()
}

// Blocking I/O
// Returns result and isFastPath
func (c *IrClient) InvokeConsensus(op []byte) (string, bool) {
	reqId := c.genReqId()

	// Propose
	retC := make(chan *ConReplyMsg, len(c.replicaAddrList))
	for _, addr := range c.replicaAddrList {
		c.wg.Add(1)
		go func(addr string) {
			defer c.wg.Done()
			reply := c.io.ProposeConOp(addr, reqId, op)
			retC <- reply
		}(addr)
	}

	// Wait for responses
	replyCount := 0
	resultSet := make(map[string]int)
	for reply := range retC {
		replyCount++
		ret := reply.Ret
		resultSet[ret]++

		if logger.IsEnabledFor(logging.DEBUG) {
			opRet := &PrepareOpRet{}
			DecodeFromStr(reply.Ret, opRet)
			//logger.Debugf("IR consensus reqId = %s result = %s (%d %d) num = %d #ofDiffReresults = %d",
			//reqId, ret, opRet.State, opRet.T, resultSet[ret], len(resultSet))
		}

		if resultSet[ret] == c.superMajority {
			// Fast path
			c.wg.Add(1)
			go func(finalRet string) {
				defer c.wg.Done()
				c.finalizeConsensusOpRet(reqId, finalRet)
			}(reply.Ret)
			//logger.Debugf("IR consensus fast path reqId = %s", reqId)
			return reply.Ret, true
		}

		if replyCount >= c.replicaN {
			// Fast path fails
			break
		}
		// NOTE: The slow path can start earlier when there are f + 1 replies for the
		// proposal, without waiting for the fast path result. However, using the
		// first available (f+1) replies, the slow path may generate a different
		// finalized result from using 2f+1 replices or a potentially successful
		// fast-path result (1/2 * f+1 replies are the same as the remaining f
		// replies).
		// To avoid this, current implementation starts the slow path after hearing
		// all replies.
	}

	// Slow path
	ret := c.app.Decide(resultSet)
	c.finalizeConsensusOpRet(reqId, ret)
	//logger.Debugf("IR consensus slow path reqId = %s", reqId)
	return ret, false
}

// Blocking
func (c *IrClient) finalizeConsensusOpRet(reqId string, ret string) {
	retC := make(chan bool, len(c.replicaAddrList))
	for _, addr := range c.replicaAddrList {
		c.wg.Add(1)
		go func(addr string) {
			defer c.wg.Done()
			c.io.FinalizeConOp(addr, reqId, ret)
			retC <- true
		}(addr)
	}

	count := 0
	for _ = range retC { // the slow path requires replies from a majority of replicas
		count++
		if count >= c.majority {
			break
		}
	}
}

func (c *IrClient) Shutdown() {
	c.wg.Wait()
}

// Helper functions
func (c *IrClient) genReqId() string {
	c.lock.Lock()
	c.reqCount++
	count := c.reqCount
	c.lock.Unlock()

	id := c.id + "-" + strconv.Itoa(count)
	return id
}
