package client

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/connection"
	"Carousel-GTS/rpc"
	"Carousel-GTS/server"
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

type Transaction struct {
	txnId        string
	commitReply  chan *rpc.CommitReply
	commitResult int
	startTime    time.Time
	endTime      time.Time
	execCount    int64
	fastPrepare  bool
	executions   []*ExecutionRecord
}

type ExecutionRecord struct {
	rpcTxn               *rpc.Transaction
	readAndPrepareReply  chan *rpc.ReadAndPrepareReply
	readKeyValueVersion  []*rpc.KeyValueVersion
	isAbort              bool
	isConditionalPrepare bool
	readFromReplica      bool
	abortReason          server.AbortReason
}

func NewExecutionRecord(rpcTxn *rpc.Transaction) *ExecutionRecord {
	e := &ExecutionRecord{
		rpcTxn:              rpcTxn,
		readAndPrepareReply: make(chan *rpc.ReadAndPrepareReply, len(rpcTxn.ParticipatedPartitionIds)),
		readKeyValueVersion: make([]*rpc.KeyValueVersion, 0),
		isAbort:             false,
	}
	return e
}

type SendOp struct {
	txnId        string
	readKeyList  []string
	writeKeyList []string
	readResult   map[string]string
	isAbort      bool
	wait         chan bool
	priority     bool
}

func (o *SendOp) BlockOwner() bool {
	return <-o.wait
}

type CommitOp struct {
	txnId         string
	writeKeyValue map[string]string
	wait          chan bool
	result        bool
	retry         bool
	waitTime      time.Duration
	expectWait    time.Duration // try to keep the target rate
}

func (o *CommitOp) BlockOwner() bool {
	return <-o.wait
}

type Client struct {
	clientId           int
	Config             configuration.Configuration
	clientDataCenterId int

	connections []connection.Connection

	sendTxnRequest   chan *SendOp
	commitTxnRequest chan *CommitOp

	txnStore map[string]*Transaction
	lock     sync.Mutex

	count          int
	timeLeg        time.Duration
	durationPerTxn time.Duration
}

func NewClient(clientId int, configFile string) *Client {
	config := configuration.NewFileConfiguration(configFile)
	queueLen := config.GetQueueLen()
	c := &Client{
		clientId:           clientId,
		Config:             config,
		clientDataCenterId: config.GetDataCenterIdByClientId(clientId),
		connections:        make([]connection.Connection, len(config.GetServerAddress())),
		sendTxnRequest:     make(chan *SendOp, queueLen),
		commitTxnRequest:   make(chan *CommitOp, queueLen),
		txnStore:           make(map[string]*Transaction),
		lock:               sync.Mutex{},
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

	return c
}

func (c *Client) Start() {
	go c.sendReadAndPrepareRequest()
	go c.sendCommitRequest()
}

func (c *Client) sendReadAndPrepareRequest() {
	for {
		op := <-c.sendTxnRequest
		if len(op.writeKeyList) == 0 && c.Config.GetIsReadOnly() &&
			(c.Config.GetServerMode() == configuration.OCC || !c.Config.GetPriority() || c.Config.GetAssignLowPriorityTimestamp()) {
			c.handleReadOnlyRequest(op)
		} else {
			c.handleReadAndPrepareRequest(op)
		}
	}
}

func (c *Client) sendCommitRequest() {
	for {
		op := <-c.commitTxnRequest
		c.handleCommitRequest(op)
	}
}

func (c *Client) getTxnId(txnId string) string {
	return "c" + strconv.Itoa(c.clientId) + "-" + txnId
}

func (c *Client) genTxnIdToServer() string {
	c.count++
	return "c" + strconv.Itoa(c.clientId) + "-" + strconv.Itoa(c.count)
}

func (c *Client) ReadAndPrepare(readKeyList []string, writeKeyList []string, txnId string, priority bool) (map[string]string, bool) {
	sendOp := &SendOp{
		txnId:        c.getTxnId(txnId),
		readKeyList:  readKeyList,
		writeKeyList: writeKeyList,
		readResult:   make(map[string]string),
		wait:         make(chan bool, 1),
		priority:     priority,
	}

	c.sendTxnRequest <- sendOp

	sendOp.BlockOwner()

	return sendOp.readResult, sendOp.isAbort
}

func (c *Client) waitReadAndPrepareRequest(op *SendOp, execution *ExecutionRecord) {
	// wait for result
	result := make(map[string]*rpc.KeyValueVersion)
	readLeader := make(map[string]bool)
	for {
		readAndPrepareReply := <-execution.readAndPrepareReply
		execution.isAbort = execution.isAbort || readAndPrepareReply.Status == int32(server.ABORT)
		execution.isConditionalPrepare = execution.isConditionalPrepare ||
			(!execution.isAbort && readAndPrepareReply.Status == int32(server.CONDITIONAL_PREPARED))
		for _, kv := range readAndPrepareReply.KeyValVerList {
			if value, exist := result[kv.Key]; exist && value.Version >= kv.Version {
				if readAndPrepareReply.IsLeader {
					readLeader[kv.Key] = readAndPrepareReply.IsLeader
				}
				continue
			}

			result[kv.Key] = kv
			readLeader[kv.Key] = readAndPrepareReply.IsLeader
			//execution.readKeyValueVersion = append(execution.readKeyValueVersion, kv)
		}
		if execution.isAbort {
			execution.abortReason = server.AbortReason(readAndPrepareReply.AbortReason)
		}

		if execution.isAbort || len(result) == len(execution.rpcTxn.ReadKeyList) {
			break
		}

	}
	op.isAbort = execution.isAbort
	if !execution.isAbort {
		for key, kv := range result {
			op.readResult[key] = kv.Value
			execution.readFromReplica = execution.readFromReplica || !readLeader[key]
			execution.readKeyValueVersion = append(execution.readKeyValueVersion, kv)
		}
	}

	op.wait <- true
}

func (c *Client) getTxnAndExecution(txnId string) (*Transaction, *ExecutionRecord) {
	c.lock.Lock()
	defer c.lock.Unlock()
	exec := c.txnStore[txnId].execCount
	return c.txnStore[txnId], c.txnStore[txnId].executions[exec]
}

func (c *Client) addTxnIfNotExist(txnId string, rpcTxn *rpc.Transaction) {
	c.lock.Lock()
	defer c.lock.Unlock()

	execution := NewExecutionRecord(rpcTxn)

	if _, exist := c.txnStore[txnId]; exist {
		// if exist increment the execution number
		c.txnStore[txnId].execCount++
		logrus.Infof("RETRY txn %v: %v", txnId, c.txnStore[txnId].execCount)
	} else {
		// otherwise add new txn
		txn := &Transaction{
			txnId:        txnId,
			commitReply:  make(chan *rpc.CommitReply, 1),
			commitResult: 0,
			startTime:    time.Now(),
			endTime:      time.Time{},
			execCount:    0,
			executions:   make([]*ExecutionRecord, 0),
		}
		c.txnStore[txnId] = txn
	}

	c.txnStore[txnId].executions = append(c.txnStore[txnId].executions, execution)
}

func (c *Client) separatePartition(op *SendOp) (map[int][][]string, map[int]bool) {
	// separate key into partitions
	partitionSet := make(map[int][][]string)
	participants := make(map[int]bool)
	if c.Config.GetServerMode() != configuration.OCC && c.Config.GetPriority() && !c.Config.GetAssignLowPriorityTimestamp() {
		for _, key := range op.readKeyList {
			pId := c.Config.GetPartitionIdByKey(key)
			logrus.Debugf("read key %v, pId %v", key, pId)
			participants[pId] = true
		}

		for _, key := range op.writeKeyList {
			pId := c.Config.GetPartitionIdByKey(key)
			logrus.Debugf("write key %v, pId %v", key, pId)
			participants[pId] = true
		}

		// if the priority optimization enable, send the all keys to partitions
		for pId := range participants {
			if _, exist := partitionSet[pId]; !exist {
				partitionSet[pId] = make([][]string, 2)
			}
			partitionSet[pId][0] = op.readKeyList
			partitionSet[pId][1] = op.writeKeyList
		}
	} else {
		for _, key := range op.readKeyList {
			pId := c.Config.GetPartitionIdByKey(key)
			logrus.Debugf("read key %v, pId %v", key, pId)
			if _, exist := partitionSet[pId]; !exist {
				partitionSet[pId] = make([][]string, 2)
			}
			partitionSet[pId][0] = append(partitionSet[pId][0], key)
			participants[pId] = true
		}

		for _, key := range op.writeKeyList {
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

func (c *Client) handleReadOnlyRequest(op *SendOp) {
	partitionSet, participants := c.separatePartition(op)
	participatedPartitions := make([]int32, len(participants))
	serverDcIds := make(map[int]bool)
	i := 0
	for pId := range participants {
		participatedPartitions[i] = int32(pId)
		serverId := c.Config.GetLeaderIdByPartitionId(pId)
		dcId := c.Config.GetDataCenterIdByServerId(serverId)
		serverDcIds[dcId] = true
		i++
	}

	t := &rpc.Transaction{
		TxnId:                    c.genTxnIdToServer(),
		ReadKeyList:              op.readKeyList,
		WriteKeyList:             op.writeKeyList,
		ParticipatedPartitionIds: participatedPartitions,
		CoordPartitionId:         int32(-1), // with read-only optimization, read-only txn does not need send to coord
		ReadOnly:                 true,
		HighPriority:             op.priority,
	}

	c.addTxnIfNotExist(op.txnId, t)

	_, execution := c.getTxnAndExecution(op.txnId)

	logrus.Debugf("txn %v client's dc %v server's dc %v txn max delay %v extra delay %v",
		op.txnId,
		c.clientDataCenterId, serverDcIds,
		c.Config.GetMaxDelay(c.clientDataCenterId, serverDcIds),
		c.Config.GetDelay())
	maxDelay := c.Config.GetMaxDelay(c.clientDataCenterId, serverDcIds).Nanoseconds()
	maxDelay += c.Config.GetDelay().Nanoseconds()
	maxDelay += time.Now().UnixNano()

	// send read and prepare request to each partition
	for pId, keyLists := range partitionSet {
		txn := &rpc.Transaction{
			TxnId:                    execution.rpcTxn.TxnId,
			ReadKeyList:              keyLists[0],
			WriteKeyList:             keyLists[1],
			ParticipatedPartitionIds: participatedPartitions,
			CoordPartitionId:         int32(-1),
			ReadOnly:                 true,
			HighPriority:             op.priority,
		}

		request := &rpc.ReadAndPrepareRequest{
			Txn:              txn,
			IsRead:           false,
			IsNotParticipant: !participants[pId],
			Timestamp:        maxDelay,
			ClientId:         "c" + strconv.Itoa(c.clientId),
		}

		// read-only txn only send to partition leader
		partitionLeaderId := c.Config.GetLeaderIdByPartitionId(pId)

		sender := NewReadAndPrepareSender(request, execution, partitionLeaderId, c)
		go sender.Send()
	}

	go c.waitReadAndPrepareRequest(op, execution)
}

func (c *Client) handleReadAndPrepareRequest(op *SendOp) {
	// separate key into partitions
	partitionSet, participants := c.separatePartition(op)

	participatedPartitions := make([]int32, len(partitionSet))
	serverDcIds := make(map[int]bool)
	i := 0
	for pId := range participants {
		participatedPartitions[i] = int32(pId)
		serverList := c.Config.GetServerIdListByPartitionId(pId)
		for _, sId := range serverList {
			dcId := c.Config.GetDataCenterIdByServerId(sId)
			serverDcIds[dcId] = true
		}

		i++
	}

	leaderIdList := c.Config.GetLeaderIdListByDataCenterId(c.clientDataCenterId)
	coordinatorPartitionId := c.Config.GetPartitionIdByServerId(leaderIdList[rand.Intn(len(leaderIdList))])
	logrus.Debugf("txn %v client datacenterId %v local leader %v coordinatorId %v",
		op.txnId, c.clientDataCenterId, leaderIdList, coordinatorPartitionId)
	for _, lId := range leaderIdList {
		pLId := c.Config.GetPartitionIdByServerId(lId)
		if _, exist := partitionSet[pLId]; exist {
			coordinatorPartitionId = pLId
			break
		}
	}

	if _, exist := partitionSet[coordinatorPartitionId]; !exist {
		partitionSet[coordinatorPartitionId] = make([][]string, 2)
	}

	t := &rpc.Transaction{
		TxnId:                    c.genTxnIdToServer(),
		ReadKeyList:              op.readKeyList,
		WriteKeyList:             op.writeKeyList,
		ParticipatedPartitionIds: participatedPartitions,
		CoordPartitionId:         int32(coordinatorPartitionId),
		ReadOnly:                 len(op.writeKeyList) == 0,
		HighPriority:             op.priority,
	}

	c.addTxnIfNotExist(op.txnId, t)

	_, execution := c.getTxnAndExecution(op.txnId)

	logrus.Debugf("txn %v client's dc %v server's dc %v txn max delay %v extra delay %v",
		op.txnId,
		c.clientDataCenterId, serverDcIds,
		c.Config.GetMaxDelay(c.clientDataCenterId, serverDcIds),
		c.Config.GetDelay())
	maxDelay := c.Config.GetMaxDelay(c.clientDataCenterId, serverDcIds).Nanoseconds()
	maxDelay += c.Config.GetDelay().Nanoseconds()
	maxDelay += time.Now().UnixNano()

	// send read and prepare request to each partition
	for pId, keyLists := range partitionSet {
		txn := &rpc.Transaction{
			TxnId:                    execution.rpcTxn.TxnId,
			ReadKeyList:              keyLists[0],
			WriteKeyList:             keyLists[1],
			ParticipatedPartitionIds: participatedPartitions,
			CoordPartitionId:         int32(coordinatorPartitionId),
			ReadOnly:                 len(op.writeKeyList) == 0,
			HighPriority:             op.priority,
		}

		request := &rpc.ReadAndPrepareRequest{
			Txn:              txn,
			IsRead:           false,
			IsNotParticipant: !participants[pId],
			Timestamp:        maxDelay,
			ClientId:         "c" + strconv.Itoa(c.clientId),
		}

		if request.IsNotParticipant || (request.Txn.ReadOnly && c.Config.GetIsReadOnly()) ||
			!c.Config.GetFastPath() {
			// only send to the leader of non-participant partition
			sId := c.Config.GetLeaderIdByPartitionId(pId)
			sender := NewReadAndPrepareSender(request, execution, sId, c)
			go sender.Send()
		} else {
			sIdList := c.Config.GetServerIdListByPartitionId(pId)
			for _, sId := range sIdList {
				sender := NewReadAndPrepareSender(request, execution, sId, c)
				go sender.Send()
			}
		}
	}

	go c.waitReadAndPrepareRequest(op, execution)
}

func (c *Client) Commit(writeKeyValue map[string]string, txnId string) (bool, bool, time.Duration, time.Duration) {
	commitOp := &CommitOp{
		txnId:         c.getTxnId(txnId),
		writeKeyValue: writeKeyValue,
		wait:          make(chan bool, 1),
		retry:         false,
		expectWait:    time.Duration(0),
	}

	c.commitTxnRequest <- commitOp
	commitOp.BlockOwner()
	return commitOp.result, commitOp.retry, commitOp.waitTime, commitOp.expectWait
}

func (c *Client) Abort(txnId string) (bool, time.Duration) {
	txn, _ := c.getTxnAndExecution(c.getTxnId(txnId))
	return c.isRetryTxn(txn.execCount + 1)
}

func (c *Client) waitCommitReply(op *CommitOp, ongoingTxn *Transaction, execution *ExecutionRecord) {
	result := <-ongoingTxn.commitReply

	ongoingTxn.endTime = time.Now()
	latency := ongoingTxn.endTime.Sub(ongoingTxn.startTime)
	if result.Result {
		ongoingTxn.commitResult = 1
		ongoingTxn.fastPrepare = result.FastPrepare
	} else {
		ongoingTxn.commitResult = 0
		execution.abortReason = server.AbortReason(result.AbortReason)
		op.retry, op.waitTime = c.isRetryTxn(ongoingTxn.execCount + 1)
	}
	op.result = result.Result
	if c.Config.GetTargetRate() > 0 {
		if op.result || c.Config.GetRetryMode() == configuration.OFF {
			op.expectWait = c.tryToMaintainTxnTargetRate(latency)
		}
	}
	op.wait <- true
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

func (c *Client) handleCommitRequest(op *CommitOp) {
	ongoingTxn, execution := c.getTxnAndExecution(op.txnId)

	if len(op.writeKeyValue) == 0 && c.Config.GetIsReadOnly() && !execution.isConditionalPrepare {
		ongoingTxn.endTime = time.Now()
		logrus.Debugf("read only txn %v commit", op.txnId)
		ongoingTxn.commitResult = 1
		op.result = true
		if c.Config.GetTargetRate() > 0 {
			latency := ongoingTxn.endTime.Sub(ongoingTxn.startTime)
			op.expectWait = c.tryToMaintainTxnTargetRate(latency)
		}
		op.wait <- true
		return
	}

	writeKeyValueList := make([]*rpc.KeyValue, len(op.writeKeyValue))
	i := 0
	for k, v := range op.writeKeyValue {
		writeKeyValueList[i] = &rpc.KeyValue{
			Key:   k,
			Value: v,
		}
		i++
	}
	readKeyVerList := make([]*rpc.KeyVersion, 0)
	// if all keys read from leader, we do not need to send read version to coordinator
	if execution.readFromReplica {
		readKeyVerList := make([]*rpc.KeyVersion, len(execution.readKeyValueVersion))
		i = 0
		for _, kv := range execution.readKeyValueVersion {
			readKeyVerList[i] = &rpc.KeyVersion{
				Key:     kv.Key,
				Version: kv.Version,
			}
			i++
		}
	}

	request := &rpc.CommitRequest{
		TxnId:            execution.rpcTxn.TxnId,
		WriteKeyValList:  writeKeyValueList,
		IsCoordinator:    false,
		ReadKeyVerList:   readKeyVerList,
		IsReadAnyReplica: execution.readFromReplica,
	}

	coordinatorId := c.Config.GetLeaderIdByPartitionId(int(execution.rpcTxn.CoordPartitionId))
	sender := NewCommitRequestSender(request, ongoingTxn, coordinatorId, c)

	go sender.Send()

	go c.waitCommitReply(op, ongoingTxn, execution)
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
		for _, ks := range txn.executions[0].rpcTxn.ReadKeyList {
			k := utils.ConvertToInt(ks)
			key[k] = true
		}

		for _, ks := range txn.executions[0].rpcTxn.WriteKeyList {
			k := utils.ConvertToInt(ks)
			key[k] = true
		}
		keyList := make([]int64, len(key))
		i := 0
		for k := range key {
			keyList[i] = k
			i++
		}

		s := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n",
			txn.executions[txn.execCount].rpcTxn.TxnId,
			txn.commitResult,
			txn.endTime.Sub(txn.startTime).Nanoseconds(),
			txn.startTime.UnixNano(),
			txn.endTime.UnixNano(),
			keyList,
			txn.execCount,
			txn.executions[txn.execCount].rpcTxn.ReadOnly,
			txn.executions[txn.execCount].rpcTxn.HighPriority,
			txn.fastPrepare,
			txn.executions[txn.execCount].abortReason,
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
