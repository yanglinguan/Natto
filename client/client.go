package client

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/connection"
	"Carousel-GTS/latencyPredictor"
	"Carousel-GTS/rpc"
	"Carousel-GTS/utils"
	"fmt"
	"github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	clientId           int
	Config             configuration.Configuration
	clientDataCenterId int

	connections []connection.Connection

	operations chan Operation

	txnStore map[string]*Transaction

	count          int
	timeLeg        time.Duration
	durationPerTxn time.Duration

	latencyPredictor *latencyPredictor.LatencyPredictor
	probeC           chan *LatInfo
	probeTimeC       chan *LatTimeInfo
}

func NewClient(clientId int, configFile string) *Client {
	config := configuration.NewFileConfiguration(configFile)
	queueLen := config.GetQueueLen()
	c := &Client{
		clientId:           clientId,
		Config:             config,
		clientDataCenterId: config.GetDataCenterIdByClientId(clientId),
		connections:        make([]connection.Connection, len(config.GetServerAddress())),
		txnStore:           make(map[string]*Transaction),
		operations:         make(chan Operation, queueLen),
		count:              0,
		timeLeg:            time.Duration(0),
	}

	if c.Config.GetTargetRate() > 0 {
		c.durationPerTxn = time.Duration(int64(time.Second) / int64(config.GetTargetRate()))
	}

	if c.Config.GetConnectionPoolSize() == 0 {
		for sId, addr := range c.Config.GetServerAddress() {
			c.connections[sId] = connection.NewSingleConnect(addr)
		}
	} else {
		for sId, addr := range c.Config.GetServerAddress() {
			c.connections[sId] = connection.NewPoolConnection(addr, c.Config.GetConnectionPoolSize())
		}
	}

	if c.Config.IsDynamicLatency() {
		c.latencyPredictor = latencyPredictor.NewLatencyPredictor(
			c.Config.GetServerAddress(),
			c.Config.GetProbeWindowLen(),
			c.Config.GetProbeWindowMinSize())
		if c.Config.IsProbeTime() {
			c.probeTimeC = make(chan *LatTimeInfo, queueLen)
		} else {
			c.probeC = make(chan *LatInfo, queueLen)
		}
	}

	return c
}

func (c *Client) Start() {
	go c.processOperation()

	if c.Config.GetServerMode() != configuration.OCC && c.Config.IsDynamicLatency() {
		if c.Config.IsProbeTime() {
			go c.probingTime()
			go c.processProbeTime()
		} else {
			go c.probing()
			go c.processProbe()
		}
	}
}

func (c *Client) processOperation() {
	for {
		op := <-c.operations
		op.Execute(c)
	}
}

func (c *Client) getTxnId(txnId string) string {
	return "c" + strconv.Itoa(c.clientId) + "-" + txnId
}

func (c *Client) genTxnIdToServer(txnId string) string {
	//c.count++
	tId := fmt.Sprintf("%v-%v", txnId, c.txnStore[txnId].execCount)
	//return "c" + strconv.Itoa(c.clientId) + "-" + strconv.Itoa(c.count)
	return tId
}

func (c *Client) ReadAndPrepare(readKeyList []string, writeKeyList []string, txnId string, priority bool) (map[string]string, bool) {
	var op ReadOp
	tId := c.getTxnId(txnId)
	if len(writeKeyList) > 0 {
		op = NewReadAndPrepareOp(tId, priority, readKeyList, writeKeyList)
	} else {
		op = NewReadOnly(tId, priority, readKeyList, writeKeyList)

	}
	c.operations <- op
	op.Block()
	return op.GetReadResult(), op.IsAbort()
}

func (c *Client) getCurrentExecutionTxnId(txnId string) string {
	exec := c.txnStore[txnId].execCount
	return c.txnStore[txnId].executions[exec].rpcTxnId
}

func (c *Client) getCurrentExecutionCount(txnId string) int64 {
	return c.txnStore[txnId].execCount
}

func (c *Client) getCurrentExecution(txnId string) *ExecutionRecord {
	currentCont := c.txnStore[txnId].execCount
	return c.txnStore[txnId].executions[currentCont]
}

func (c *Client) getExecution(txnId string, count int64) *ExecutionRecord {
	return c.txnStore[txnId].executions[count]
}

func (c *Client) getTxn(txnId string) *Transaction {
	return c.txnStore[txnId]
}

func (c *Client) getMaxDelay(serverIdList []int, serverDcIds map[int]bool) int64 {
	var maxDelay int64 = 0
	if c.Config.IsDynamicLatency() {
		maxDelay = c.predictOneWayLatency(serverIdList) * 1000000 // change to nanoseconds
		maxDelay += c.Config.GetDelay().Nanoseconds()
		maxDelay += time.Now().UnixNano()
	} else {
		maxDelay = c.Config.GetMaxDelay(c.clientDataCenterId, serverDcIds).Nanoseconds()
		maxDelay += c.Config.GetDelay().Nanoseconds()
		maxDelay += time.Now().UnixNano()
	}

	return maxDelay
}

func (c *Client) addTxnIfNotExist(op ReadOp) {

	txnId := op.GetTxnId()
	//rpcTxnId := c.genTxnIdToServer(txnId)

	var rpcTxnId string
	if _, exist := c.txnStore[txnId]; exist {
		// if exist increment the execution number
		rpcTxnId = c.genTxnIdToServer(txnId)
		c.txnStore[txnId].execCount++
		op.ClearReadKeyList()
		op.ClearWriteKeyList()
		logrus.Debugf("RETRY txn %v: %v", txnId, c.txnStore[txnId].execCount)
	} else {
		// otherwise add new txn
		c.txnStore[txnId] = NewTransaction(op, c)
		rpcTxnId = c.genTxnIdToServer(txnId)
	}

	execution := NewExecutionRecord(op, rpcTxnId, len(c.txnStore[txnId].readKeyList))

	c.txnStore[txnId].executions = append(c.txnStore[txnId].executions, execution)
}

func (c *Client) AddOperation(op Operation) {
	c.operations <- op
}

func (c *Client) Commit(writeKeyValue map[string]string, txnId string) (bool, bool, time.Duration, time.Duration) {
	tId := c.getTxnId(txnId)
	var commitOp CommitOp
	if len(c.txnStore[tId].writeKeyList) == 0 && c.Config.GetIsReadOnly() {
		commitOp = NewCommitReadOnlyOp(tId)
	} else {
		commitOp = NewCommitOp(tId, writeKeyValue)
	}

	c.getCurrentExecution(tId).setCommitOp(commitOp)
	c.AddOperation(commitOp)

	commitOp.Block()

	return commitOp.GetResult()
}

func (c *Client) Abort(txnId string) (bool, time.Duration) {
	tId := c.getTxnId(txnId)
	op := NewAbortOp(tId)
	c.AddOperation(op)

	op.Block()
	return op.isRetry, op.waitTime
}

func (c *Client) isRetryTxn(execNum int64) (bool, time.Duration) {
	if c.Config.GetRetryMode() == configuration.OFF ||
		(c.Config.GetMaxRetry() >= 0 && execNum > c.Config.GetMaxRetry()) {
		return false, 0
	}

	waitTime := c.Config.GetRetryInterval()

	if c.Config.GetRetryMode() == configuration.EXP {
		//exponential back-off
		abortNum := execNum
		n := int64(math.Exp2(float64(abortNum)))
		randomFactor := c.Config.GetRetryMaxSlot()
		if n > 0 {
			randomFactor = rand.Int63n(n)
		}
		if randomFactor > c.Config.GetRetryMaxSlot() {
			randomFactor = c.Config.GetRetryMaxSlot()
		}
		waitTime = c.Config.GetRetryInterval() * time.Duration(randomFactor)
	}
	return true, waitTime
}

func (c *Client) PrintServerStatus(commitTxn []int) {
	var wg sync.WaitGroup
	if c.Config.GetReplication() {
		for sId := range c.connections {
			pId := c.Config.GetPartitionIdByServerId(sId)
			committed := commitTxn[pId]
			request := &rpc.PrintStatusRequest{
				CommittedTxn: int32(committed),
			}
			sender := NewPrintStatusRequestSender(request, sId, c)
			wg.Add(1)
			go sender.Send(&wg)
		}
	} else {
		totalPartition := c.Config.GetTotalPartition()
		for pId := 0; pId < totalPartition; pId++ {
			sId := c.Config.GetLeaderIdByPartitionId(pId)
			committed := commitTxn[pId]
			request := &rpc.PrintStatusRequest{
				CommittedTxn: int32(committed),
			}
			sender := NewPrintStatusRequestSender(request, sId, c)
			wg.Add(1)
			go sender.Send(&wg)
		}
	}
	wg.Wait()
}

func (c *Client) PrintTxnStatisticData() {
	file, err := os.Create("c" + strconv.Itoa(c.clientId) + ".statistic")
	if err != nil || file == nil {
		logrus.Fatal("Fails to create log file: statistic.log")
		return
	}

	_, err = file.WriteString("#txnId, commit result, latency, start time, end time, keys\n")
	if err != nil {
		logrus.Fatalf("Cannot write to file, %v", err)
		return
	}

	for _, txn := range c.txnStore {
		key := make(map[int64]bool)
		for _, ks := range txn.readKeyList {
			k := utils.ConvertToInt(ks)
			key[k] = true
		}

		for _, ks := range txn.writeKeyList {
			k := utils.ConvertToInt(ks)
			key[k] = true
		}
		keyList := make([]int64, len(key))
		i := 0
		for k := range key {
			keyList[i] = k
			i++
		}

		s := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v\n",
			txn.txnId,
			txn.commitResult,
			txn.executions[txn.execCount].endTime.Sub(txn.startTime).Nanoseconds(),
			txn.startTime.UnixNano(),
			txn.executions[txn.execCount].endTime.UnixNano(),
			keyList,
			txn.execCount,
			txn.isReadOnly(),
			txn.priority,
		)
		_, err = file.WriteString(s)
		if err != nil {
			logrus.Fatalf("Cannot write to file %v", err)
		}
	}

	err = file.Close()
	if err != nil {
		logrus.Fatalf("cannot close file %v", err)
	}
}

func (c *Client) HeartBeat(dstServerId int) int {
	sender := NewHeartBeatSender(dstServerId, c)
	return sender.Send()
}

func (c *Client) tryToMaintainTxnTargetRate(latency time.Duration) time.Duration {
	if latency < c.durationPerTxn {
		expectWait := c.durationPerTxn - latency

		if c.timeLeg <= expectWait {
			expectWait -= c.timeLeg
			c.timeLeg = 0
		} else {
			c.timeLeg -= expectWait
			expectWait = 0
		}

		if expectWait > 0 {
			return expectWait
		} else {
			return 0
		}
	} else {
		leg := latency - c.durationPerTxn
		if tmp := c.timeLeg + leg; tmp > c.timeLeg {
			// Overflow
			c.timeLeg = tmp
		}
		return 0
	}
}

func (c *Client) separatePartition(op ReadOp) (map[int][][]string, map[int]bool) {
	// separate key into partitions
	partitionSet := make(map[int][][]string)
	participants := make(map[int]bool)
	if c.Config.GetServerMode() != configuration.OCC &&
		c.Config.GetPriority() && c.Config.IsEarlyAbort() {
		for _, key := range op.GetReadKeyList() {
			pId := c.Config.GetPartitionIdByKey(key)
			logrus.Debugf("read key %v, pId %v", key, pId)
			participants[pId] = true
		}

		for _, key := range op.GetWriteKeyList() {
			pId := c.Config.GetPartitionIdByKey(key)
			logrus.Debugf("write key %v, pId %v", key, pId)
			participants[pId] = true
		}

		// if the priority optimization enable, send the all keys to partitions
		for pId := range participants {
			if _, exist := partitionSet[pId]; !exist {
				partitionSet[pId] = make([][]string, 2)
			}
			partitionSet[pId][0] = op.GetReadKeyList()
			partitionSet[pId][1] = op.GetWriteKeyList()
		}
	} else {
		for _, key := range op.GetReadKeyList() {
			pId := c.Config.GetPartitionIdByKey(key)
			logrus.Debugf("read key %v, pId %v", key, pId)
			if _, exist := partitionSet[pId]; !exist {
				partitionSet[pId] = make([][]string, 2)
			}
			partitionSet[pId][0] = append(partitionSet[pId][0], key)
			participants[pId] = true
		}

		for _, key := range op.GetWriteKeyList() {
			pId := c.Config.GetPartitionIdByKey(key)
			logrus.Debugf("write key %v, pId %v", key, pId)
			if _, exist := partitionSet[pId]; !exist {
				partitionSet[pId] = make([][]string, 2)
			}
			partitionSet[pId][1] = append(partitionSet[pId][1], key)
			participants[pId] = true
		}

	}

	return partitionSet, participants

}

func (c *Client) getParticipantPartition(participants map[int]bool) ([]int32, map[int]bool, []int) {
	participatedPartitions := make([]int32, len(participants))
	serverDcIds := make(map[int]bool)
	serverIdList := make([]int, 0)
	i := 0
	for pId := range participants {
		participatedPartitions[i] = int32(pId)
		serverList := c.Config.GetServerIdListByPartitionId(pId)
		for _, sId := range serverList {
			serverIdList = append(serverIdList, sId)
			dcId := c.Config.GetDataCenterIdByServerId(sId)
			serverDcIds[dcId] = true
		}
		i++
	}

	return participatedPartitions, serverDcIds, serverIdList
}
