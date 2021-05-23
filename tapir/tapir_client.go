package tapir

import (
	"math"
	"strconv"
	sync "sync"
	"time"

	"github.com/op/go-logging"
)

type TapirClient struct {
	irClientTable map[int32]*IrClient // partitionId --> IR client for the partition's replicas

	id       string
	txnCount int
	lock     sync.Mutex

	// Transaction parameters
	retryN int // max number of reties for txn prepare in TAPIR

	// replica info
	pReplicaTable   map[int32][]string // partitionId --> a list of replica addresses (ip:port)
	pClosestReplica map[int32]string   // partitionId --> closest replica's address (ip:port)
	majority        int                // NOTE: assuming partitions have the same number of replicas

	io IO // Network I/O interface

	wg sync.WaitGroup // tranxaction wait group

	// Clock skew
	clockSkew int64

	// Sharding manager: maps keys to partitions
	sm TapirShardMngr
}

func NewTapirClient(
	id string, // client id
	prepareRetryN int, // TAPIR protocols' txn prepare retry limit
	pReplicaTable map[int32][]string, // partitionId --> a list of replica addresses (ip:port)
	//pClosestReplica map[int32]string, // partitionId --> closest replica's address (ip:port)
	clockSkew int64,
) *TapirClient {
	c := &TapirClient{
		irClientTable:   make(map[int32]*IrClient),
		id:              id,
		txnCount:        0,
		retryN:          prepareRetryN,
		pReplicaTable:   pReplicaTable,
		pClosestReplica: make(map[int32]string),
		majority:        0,
		io:              NewGrpcIo(),
		clockSkew:       clockSkew,
		sm:              nil,
	}

	for pId, rAddrList := range pReplicaTable {
		c.pClosestReplica[pId] = rAddrList[0] // temporary closest replica
		logger.Debugf("initally set: partitionId = %d closest replica = %s", pId, rAddrList[0])
		c.irClientTable[pId] = NewIrClient(c, c.id, rAddrList, c.io)
		c.majority = len(rAddrList)>>1 + 1 // TODO allow partitions have different number of replicas
	}

	return c
}

// Customized mapping from keys to partitions
func (c *TapirClient) RegisterShardMngr(sm TapirShardMngr) {
	c.sm = sm
}

func (c *TapirClient) Start() {
	addrList := make([]string, 0)
	for _, rList := range c.pReplicaTable {
		for _, addr := range rList {
			addrList = append(addrList, addr)
		}
	}
	// Connect to every data server
	c.io.InitConn(addrList) // Blocking

	// Keeps finding the closest replica for every partition for 2 seconds
	go c.startFindingClosestReplicas(TAPIR_TIME_TO_FIND_CLOSEST_REPLICAS)
}

type ReplicaLat struct {
	addr string
	lat  int64
}

// Keeps finding the closest replica for every partition for a given time period (unit: second)
func (c *TapirClient) startFindingClosestReplicas(t int64) {
	start := time.Now().Unix()
	for time.Now().Unix()-start < t {
		cr := c.findClosestReplicas()
		c.pClosestReplica = cr // NOTE: other threads may concurrently read this variable
	}
}

func (c *TapirClient) Shutdown() {
	c.wg.Wait()
	for _, irClient := range c.irClientTable {
		irClient.Shutdown()
	}
	c.io.Shutdown()
}

// IR app interface
func (c *TapirClient) Decide(resultTable map[string]int) string {
	prepareRet := make(map[int]int) // prepare result state --> count
	maxRetryT := int64(TAPIR_VERSION_INVALID)
	// Decoding
	for ret, count := range resultTable {
		pRet := &PrepareOpRet{}
		DecodeFromStr(ret, pRet)

		// Retry responses may have different timestamps and thus []byte result
		if pRet.State == TAPIR_TXN_RETRY {
			if pRet.T > maxRetryT {
				maxRetryT = pRet.T
			}
		}
		if _, ok := prepareRet[pRet.State]; !ok {
			prepareRet[pRet.State] = 0
		}
		prepareRet[pRet.State] += count
	}

	ret := TAPIR_TXN_ABORT
	retryT := int64(TAPIR_VERSION_INVALID)
	if _, ok := prepareRet[TAPIR_TXN_ABORT]; ok {
		ret = TAPIR_TXN_ABORT
	} else if count, ok := prepareRet[TAPIR_TXN_PREPARE_OK]; ok && count >= c.majority {
		ret = TAPIR_TXN_PREPARE_OK
	} else if count, ok := prepareRet[TAPIR_TXN_ABSTAIN]; ok && count >= c.majority {
		ret = TAPIR_TXN_ABORT
	} else if _, ok := prepareRet[TAPIR_TXN_RETRY]; ok {
		ret = TAPIR_TXN_RETRY
		retryT = maxRetryT
	}

	pFinalRet := &PrepareOpRet{State: ret, T: retryT}
	return EncodeToStr(pFinalRet)
}

// An example for executing a txn
// Returns true and read results if the txn is committed, otherwise returns false
func (c *TapirClient) ExecTxn(
	rSet map[int32][]string,
	wSet map[int32]map[string]string,
) (string, bool, bool, map[string]string) {
	txn := c.Begin()
	ret := c.ReadMultiPartitions(txn, rSet)
	//logger.Debugf("txnId = %s write = %v", txn.getId(), wSet)
	c.WriteMultiPartitions(txn, wSet)
	isCommit, isFast := c.Commit(txn)
	logger.Debugf("txnId = %s isCommit = %t isFastPath = %t", txn.getId(), isCommit, isFast)
	//logger.Debugf("txnId = %s read results = %v", txn.getId(), ret)
	if isCommit {
		return txn.getId(), true, isFast, ret
	}
	return txn.getId(), false, false, nil
}

//// Transactional interface

// Begins a txn context
func (c *TapirClient) Begin() *Txn {
	tId := c.genTxnId()
	return NewTxn(tId)
}

//// Read/write interfaces using customized key-partition mapping manager
//func (c *TapirClient) ReadMultiKeys(t *Txn, readKeyList []string) map[string]string {
//	pReadKey := c.sm.MapKeyListToShard(readKeyList)
//	return c.ReadMultiPartitions(t, pReadKey)
//}
//
//func (c *TapirClient) WriteMultiKeys(t *Txn, writeKeySet map[string]string) {
//	pWriteSet := c.sm.MapKeySetToShard(writeKeySet)
//	c.WriteMultiPartitions(t, pWriteSet)
//}

// Not thread-safe
func (c *TapirClient) ReadMultiPartitions(
	t *Txn,
	pReadKey map[int32][]string, // paritiontId --> read key list
) map[string]string {
	tId := t.getId()
	readRet := make(map[string]string)
	readReqN := 0
	retC := make(chan *partitionReadResult, len(pReadKey))

	for pId, rKeyList := range pReadKey {
		rList := make([]string, 0, len(rKeyList))
		// Reads local buffered writes and reads
		for _, rKey := range rKeyList {
			if rVal, ok := t.readBufferData(pId, rKey); ok {
				readRet[rKey] = rVal
			} else {
				rList = append(rList, rKey)
			}
		}

		if len(rList) != 0 {
			// Sends read requests to data servers
			readReqN++
			go func(pId int32, rList []string) {
				result := c.doReadPartition(tId, pId, rList)
				retC <- &partitionReadResult{pId, result}
			}(pId, rList)
		}
	}

	// Process read replies
	for ret := range retC {
		t.addReadResult(ret.pId, ret.result)
		for k, vv := range ret.result {
			readRet[k] = vv.Val
		}
		readReqN--
		if readReqN <= 0 {
			break
		}
	}

	return readRet
}

func (c *TapirClient) WriteMultiPartitions(
	t *Txn,
	pWriteSet map[int32]map[string]string, // partitionId --> write key --> value
) {
	for pId, wData := range pWriteSet {
		t.writeData(pId, wData)
	}
}

// Clients commit
// Returns isCommitted, isFastPath (for all participant partitions)
func (c *TapirClient) Commit(t *Txn) (bool, bool) {
	t.complete() // No more reads/writes are allowed

	count := 0
	ret := TAPIR_TXN_RETRY
	retryT := int64(0)
	isFast := false

	var ts int64
	for ret == TAPIR_TXN_RETRY && count <= c.retryN {
		ts = c.genTxnTimestamp()
		for ts <= retryT {
			ts = c.genTxnTimestamp() + (retryT - ts)
		}
		ret, retryT, isFast = c.prepareTxn(t, ts) // Prepares the txn
		count++
	}

	isToCommit := false
	if ret == TAPIR_TXN_PREPARE_OK {
		isToCommit = true
	}
	// Asynchronously commits or aborts the txn
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.commitTxn(isToCommit, t, ts)
	}()
	return isToCommit, isFast
}

// Clients abort
func (c *TapirClient) Abort(t *Txn) {
	t.complete() // No more reads/writes are allowed
	ts := c.genTxnTimestamp()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.commitTxn(false, t, ts) // Abort (IR inconsistent)
	}()
}

// Returns txn prepare result, retry timestamp (if retry), and isFastPath
func (c *TapirClient) prepareTxn(t *Txn, timestamp int64) (int, int64, bool) {
	tId := t.getId()
	pRwTable := t.getRwSet()
	retC := make(chan *prepareResult, len(pRwTable))
	for pId, rwSet := range pRwTable {
		c.wg.Add(1)
		go func(pId int32, wSet map[string]string, rSet map[string]int64) {
			defer c.wg.Done()
			pRet, isFast := c.doPreparePartition(pId, tId, timestamp, wSet, rSet)
			retC <- &prepareResult{pId: pId, result: pRet, isFast: isFast}
		}(pId, rwSet.GetWriteSet(), rwSet.GetReadSet())
	}

	count := 0
	isFast := true
	for ret := range retC {
		count++
		isFast = isFast && ret.isFast
		if ret.result.State == TAPIR_TXN_ABORT || ret.result.State == TAPIR_TXN_ABSTAIN {
			return TAPIR_TXN_ABORT, TAPIR_VERSION_INVALID, isFast
		} else if ret.result.State == TAPIR_TXN_RETRY {
			return TAPIR_TXN_RETRY, ret.result.T, isFast
		}
		if count >= len(pRwTable) {
			break
		}
	}
	return TAPIR_TXN_PREPARE_OK, TAPIR_VERSION_INVALID, isFast
}

func (c *TapirClient) commitTxn(isToCommit bool, t *Txn, timestamp int64) {
	// Commit (IR inconsistent)
	tId := t.getId()
	pRwTable := t.getRwSet()
	for pId, rwSet := range pRwTable {
		c.wg.Add(1)
		go func(pId int32, wSet map[string]string, rSet map[string]int64) {
			defer c.wg.Done()
			c.doCommitPartition(pId, isToCommit, tId, timestamp, wSet, rSet)
		}(pId, rwSet.GetWriteSet(), rwSet.GetReadSet())
	}
}

// Helper data structures and functions
type partitionReadResult struct {
	pId    int32
	result map[string]*VerVal
}

type prepareResult struct {
	pId    int32
	result *PrepareOpRet
	isFast bool // isFastPath
}

func (c *TapirClient) doReadPartition(
	txnId string,
	pId int32,
	rKeyList []string,
) map[string]*VerVal {
	logger.Debugf("txnId = %s reads from partitionId = %d", txnId, pId)
	irClient := c.getIrClient(pId)
	addr := c.getClosestReplicaAddr(pId)
	rOp := &ReadOp{Id: txnId, Key: rKeyList}
	op := Encode(rOp)

	ret := irClient.InovkeUnlog(addr, op)

	rOpRet := &ReadOpRet{}
	Decode(ret, rOpRet)

	return rOpRet.Ret
}

func (c *TapirClient) doPreparePartition(
	pId int32,
	txnId string,
	timestamp int64,
	writeSet map[string]string,
	readSet map[string]int64,
) (*PrepareOpRet, bool) {
	// Prepare (IR consensus)
	irClient := c.getIrClient(pId)
	pOp := &PrepareOp{T: timestamp, Id: txnId, Write: writeSet, Read: readSet}
	op := Encode(pOp)

	ret, isFast := irClient.InvokeConsensus(op)

	pOpRet := &PrepareOpRet{}
	DecodeFromStr(ret, pOpRet)

	return pOpRet, isFast
}

func (c *TapirClient) doCommitPartition(
	pId int32,
	isToCommit bool,
	txnId string,
	timestamp int64,
	writeSet map[string]string,
	readSet map[string]int64,
) {
	// Commit / Abort (IR inconsistent)
	irClient := c.getIrClient(pId)
	cOp := &CommitOp{
		IsC:   isToCommit,
		T:     timestamp,
		Id:    txnId,
		Write: writeSet,
		Read:  readSet,
	}
	op := Encode(cOp)
	logger.Debugf("TAPIR invokes IR inconsistent txnId = %s isCommit = %t partitionId = %d", txnId, isToCommit, pId)
	irClient.InvokeInconsistent(op)
}

func (c *TapirClient) genTxnId() string {
	c.lock.Lock()
	c.txnCount++
	count := c.txnCount
	c.lock.Unlock()

	id := c.id + "-" + strconv.Itoa(count)
	return id
}

func (c *TapirClient) genTxnTimestamp() int64 {
	//return time.Now().UnixNano()
	return ReadClockNano(GenClockSkew(c.clockSkew))
}

func (c *TapirClient) getIrClient(pId int32) *IrClient {
	irClient, ok := c.irClientTable[pId]
	if !ok {
		logger.Fatalf("Unknown partitionId = %d", pId)
	}
	return irClient
}
func (c *TapirClient) getClosestReplicaAddr(pId int32) string {
	addr, ok := c.pClosestReplica[pId]
	if !ok {
		logger.Fatalf("Unknown partitionId = %d", pId)
	}
	return addr
}

// Finds the closest replica for every partition
func (c *TapirClient) findClosestReplicas() map[int32]string {
	var wg sync.WaitGroup
	var lock sync.Mutex
	closestR := make(map[int32]string)
	for pId, _ := range c.pReplicaTable {
		wg.Add(1)
		go func(pId int32) {
			defer wg.Done()
			addr := c.getClosestReplica(pId)
			lock.Lock()
			closestR[pId] = addr
			lock.Unlock()
		}(pId)
	}
	wg.Wait()
	if logger.IsEnabledFor(logging.DEBUG) {
		for pId, addr := range closestR {
			logger.Debugf("detect: partitionId = %d closest replica = %s", pId, addr)
		}
	}
	return closestR
}

// Finds the closest replica of a given partition
func (c *TapirClient) getClosestReplica(pId int32) string {
	rList := c.pReplicaTable[pId]
	retC := make(chan *ReplicaLat, len(rList))
	for _, addr := range rList {
		go func(addr string) {
			lat := c.pingReplica(addr)
			retC <- &ReplicaLat{addr, lat}
		}(addr)
	}

	closestR := ""
	minLat := int64(math.MaxInt64)

	count := 0
	for rl := range retC {
		count++
		if rl.lat <= minLat {
			minLat = rl.lat
			closestR = rl.addr
		}
		if count >= len(rList) {
			break
		}
	}
	return closestR
}

// Returns ping latency (ns) to the given server
func (c *TapirClient) pingReplica(addr string) int64 {
	start := time.Now().UnixNano()
	c.io.Probe(addr)
	lat := time.Now().UnixNano() - start
	return lat
}

// For testing
func (c *TapirClient) Test() {
	var wg sync.WaitGroup
	for _, addrList := range c.pReplicaTable {
		for _, addr := range addrList {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				logger.Debugf("Sending test request to %s", addr)
				c.io.Test(addr)
			}(addr)
		}
	}
	wg.Wait()
}
