package client

import (
	"Carousel-GTS/benchmark/workload"
	"Carousel-GTS/configuration"
	"Carousel-GTS/connection"
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"io"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

type IFClient interface {
	Start()
	ExecTxn(txn workload.Txn) (bool, bool, time.Duration, time.Duration)
	Close()
}

type Client struct {
	clientId           int
	Config             configuration.Configuration
	clientDataCenterId int

	connections                     []connection.Connection
	readResultFromCoordinatorStream []rpc.Carousel_ReadResultFromCoordinatorClient

	operations chan Operation

	//txnStore map[string]*Transaction
	txnStore *TxnStore

	count          int
	timeLeg        time.Duration
	durationPerTxn time.Duration

	networkMeasureConnection connection.Connection

	lock   sync.Mutex
	delays []int64
}

func NewClient(clientId int, configFile string) *Client {
	config := configuration.NewFileConfiguration(configFile)
	queueLen := config.GetQueueLen()
	c := &Client{
		clientId:           clientId,
		Config:             config,
		clientDataCenterId: config.GetDataCenterIdByClientId(clientId),
		connections:        make([]connection.Connection, len(config.GetServerAddress())),
		//readAndPrepareRequestStream: make([]rpc.Carousel_ReadAndPrepareClient, len(config.GetServerAddress())),
		txnStore:   NewTxnStore(),
		operations: make(chan Operation, queueLen),
		count:      0,
		timeLeg:    time.Duration(0),
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
	if c.Config.IsDynamicLatency() && c.Config.UseNetworkTimestamp() {
		c.networkMeasureConnection = connection.NewSingleConnect(config.GetNetworkMeasureAddr(c.clientDataCenterId))
	}

	return c
}

func (c *Client) createReadResultFromCoordinatorStream() {
	// create stream
	c.readResultFromCoordinatorStream = make(
		[]rpc.Carousel_ReadResultFromCoordinatorClient,
		c.Config.GetTotalPartition())
	for i, sId := range c.Config.GetExpectPartitionLeaders() {
		addr := c.Config.GetServerAddressByServerId(sId)
		conn := connection.NewSingleConnect(addr)
		client := rpc.NewCarouselClient(conn.GetConn())
		in := &rpc.ReadRequestToCoordinator{
			ClientId: strconv.Itoa(c.clientId),
		}
		logrus.Debugf("send read result from coord to server %v, client %v", sId, in.ClientId)
		stream, err := client.ReadResultFromCoordinator(context.Background(), in)
		if err != nil {
			logrus.Fatalf("open stream error %v", err)
		}
		c.readResultFromCoordinatorStream[i] = stream
	}
}

func (c *Client) getTxnIdByServerTxnId(txnId string) string {
	items := strings.Split(txnId, "-")
	id := ""
	for i := 0; i < len(items)-1; i++ {
		id += items[i] + "-"
	}
	return id[:len(id)-1]
}

func (c *Client) receiveReadResultFromCoordinatorStream(i int) {
	for {
		resp, err := c.readResultFromCoordinatorStream[i].Recv()
		if err == io.EOF {
			//done <- true //means stream is finished
			return
		}
		if err != nil {
			logrus.Fatalf("cannot receive %v", err)
		}

		logrus.Debugf("Receive read result from coor: %v from partition %v leader",
			resp.TxnId, i)
		if resp.TxnId == "ACK" {
			logrus.Debugf("receive ack")
			continue
		}
		op := NewReadReplyFromCoordinatorOp(resp)
		c.AddOperation(op)
	}
}

// one goroutine processing the operation
// one goroutine probing the network latency
func (c *Client) Start() {
	// create stream to coordinators
	// receiving result from coordinators
	go c.processOperation()
	if c.Config.ForwardReadToCoord() {
		c.createReadResultFromCoordinatorStream()
		for i := range c.readResultFromCoordinatorStream {
			go c.receiveReadResultFromCoordinatorStream(i)
		}
	}

	if c.Config.IsDynamicLatency() && c.Config.UseNetworkTimestamp() {
		c.delays = make([]int64, c.Config.GetServerNum())
		go c.predictDelay()
	}
}

func (c *Client) processOperation() {
	for {
		op := <-c.operations
		op.Execute(c)
	}
}

func getTxnId(clientId int, txnId string) string {
	return strconv.Itoa(clientId) + "-" + txnId
}

func (c *Client) ExecTxn(txn workload.Txn) (bool, bool, time.Duration, time.Duration) {
	readResult, isAbort := c.ReadAndPrepare(txn.GetReadKeys(), txn.GetWriteKeys(), txn.GetTxnId(), txn.GetPriority())

	if isAbort {
		retry, waitTime := c.Abort(txn.GetTxnId())
		return false, retry, waitTime, 0
	}

	txn.GenWriteData(readResult)

	if logrus.IsLevelEnabled(logrus.DebugLevel) {

		for k, v := range txn.GetWriteData() {
			logrus.Debugf("txn %v write key %v: %v", txn.GetTxnId(), k, v)
		}
	}

	return c.Commit(txn.GetWriteData(), txn.GetTxnId())
}

func (c *Client) Close() {
	c.PrintTxnStatisticData()
}

func (c *Client) ReadAndPrepare(readKeyList []string, writeKeyList []string, txnId string, priority bool) (map[string]string, bool) {
	var op ReadOp
	// append the client
	tId := getTxnId(c.clientId, txnId)
	if len(writeKeyList) > 0 {
		op = NewReadAndPrepareOp(tId, priority, readKeyList, writeKeyList)
	} else {
		op = NewReadOnly(tId, priority, readKeyList, writeKeyList)

	}
	c.operations <- op
	// block the client until get the read result from the server
	op.Block()
	return op.GetReadResult(), op.IsAbort()
}

func (c *Client) getExecutionCountByTxnId(txnId string) int64 {
	list := strings.Split(txnId, "-")
	exeCountStr := list[len(list)-1]
	count, _ := strconv.Atoi(exeCountStr)
	return int64(count)
}

func (c *Client) predictDelay() {
	netClient := rpc.NewNetworkMeasureClient(c.networkMeasureConnection.GetConn())
	request := &rpc.LatencyRequest{
		Per: int32(c.Config.GetPredictDelayPercentile()),
	}
	updateInterval := c.Config.GetUpdateInterval()
	for {
		reply, err := netClient.PredictLatency(context.Background(), request)
		if err != nil {
			logrus.Fatalf("cannot get predict delay %v", err)
		}
		logrus.Debugf("delay %v", reply.Delays)
		c.lock.Lock()
		for i, d := range reply.Delays {
			c.delays[i] = d
		}
		c.lock.Unlock()
		time.Sleep(updateInterval)
	}
}

func (c *Client) getMaxDelay(serverIdList []int, serverDcIds map[int]bool) int64 {
	var maxDelay int64 = 0
	if !c.Config.UseNetworkTimestamp() {
		//maxDelay = time.Now().UnixNano()
		return maxDelay
	}

	if c.Config.IsDynamicLatency() {
		maxDelay = c.predictOneWayLatency(serverIdList) * 1000000 // change to nanoseconds
		maxDelay += c.Config.GetDelay().Nanoseconds()
	} else {
		maxDelay = c.Config.GetMaxDelay(c.clientDataCenterId, serverDcIds).Nanoseconds()
		maxDelay += c.Config.GetDelay().Nanoseconds()
	}

	return maxDelay
}

func (c *Client) getEstimateArrivalTime(participantPartitions []int32) []int64 {
	serverList := make([]int, len(participantPartitions))
	var result []int64
	if !c.Config.UseNetworkTimestamp() {
		//maxDelay = time.Now().UnixNano()
		return result
	}
	for i, pId := range participantPartitions {
		leaderId := c.Config.GetLeaderIdByPartitionId(int(pId))
		serverList[i] = leaderId
	}
	if c.Config.IsDynamicLatency() {
		result = c.estimateArrivalTime(serverList)
	} else {
		result = c.Config.GetLatencyList(c.clientDataCenterId, serverList)
	}
	now := time.Now().UnixNano()
	for i := range result {
		result[i] += now
	}
	return result
}

func (c *Client) addTxnIfNotExist(op ReadOp) {
	c.txnStore.addTxn(
		op,
		op.GetTxnId(),
		op.GetReadKeyList(),
		op.GetWriteKeyList(),
		op.GetPriority(),
		c.Config)
}

func (c *Client) AddOperation(op Operation) {
	c.operations <- op
}

func (c *Client) Commit(writeKeyValue map[string]string, txnId string) (bool, bool, time.Duration, time.Duration) {
	tId := getTxnId(c.clientId, txnId)
	commitOp := NewCommitOp(tId, writeKeyValue)
	logrus.Debugf("create commit op txn %v", tId)
	c.AddOperation(commitOp)

	commitOp.Block()

	return commitOp.GetResult()
}

func (c *Client) Abort(txnId string) (bool, time.Duration) {
	tId := getTxnId(c.clientId, txnId)
	op := NewAbortOp(tId)
	c.AddOperation(op)

	op.Block()
	return op.isRetry, op.waitTime
}

func isRetryTxn(execNum int64, config configuration.Configuration) (bool, time.Duration) {
	if config.GetRetryMode() == configuration.OFF ||
		(config.GetMaxRetry() >= 0 && execNum > config.GetMaxRetry()) {
		return false, 0
	}

	waitTime := config.GetRetryInterval()

	if config.GetRetryMode() == configuration.EXP {
		//exponential back-off
		abortNum := execNum
		n := int64(math.Exp2(float64(abortNum)))
		randomFactor := config.GetRetryMaxSlot()
		if n > 0 {
			randomFactor = rand.Int63n(n)
		}
		if randomFactor > config.GetRetryMaxSlot() {
			randomFactor = config.GetRetryMaxSlot()
		}
		waitTime = config.GetRetryInterval() * time.Duration(randomFactor)
	}
	return true, waitTime
}

func (c *Client) PrintServerStatus(commitTxn []int) {
	var wg sync.WaitGroup
	if c.Config.GetReplication() {
		for sId := range c.connections {
			if c.Config.IsCoordServer(sId) {
				continue
			}
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
	c.txnStore.PrintTxnStatisticData(c.clientId)
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

func separatePartition(
	readKeyList []string, writeKeyList []string, config configuration.Configuration) (map[int][][]string, map[int]bool) {
	// separate key into partitions
	partitionSet := make(map[int][][]string)
	participants := make(map[int]bool)
	if config.GetServerMode() == configuration.PRIORITY && (config.IsEarlyAbort() || config.IsOptimisticReorder()) {
		for _, key := range readKeyList {
			pId := config.GetPartitionIdByKey(key)
			logrus.Debugf("read key %v, pId %v", key, pId)
			participants[pId] = true
		}

		for _, key := range writeKeyList {
			pId := config.GetPartitionIdByKey(key)
			logrus.Debugf("write key %v, pId %v", key, pId)
			participants[pId] = true
		}

		// if the priority optimization enable, send the all keys to partitions
		for pId := range participants {
			if _, exist := partitionSet[pId]; !exist {
				partitionSet[pId] = make([][]string, 2)
			}
			partitionSet[pId][0] = readKeyList
			partitionSet[pId][1] = writeKeyList
		}
	} else {
		for _, key := range readKeyList {
			pId := config.GetPartitionIdByKey(key)
			logrus.Debugf("read key %v, pId %v", key, pId)
			if _, exist := partitionSet[pId]; !exist {
				partitionSet[pId] = make([][]string, 2)
			}
			partitionSet[pId][0] = append(partitionSet[pId][0], key)
			participants[pId] = true
		}

		for _, key := range writeKeyList {
			pId := config.GetPartitionIdByKey(key)
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

func getParticipantPartition(
	participants map[int]bool,
	config configuration.Configuration) ([]int32, map[int]bool, []int) {
	participatedPartitions := make([]int32, len(participants))
	serverDcIds := make(map[int]bool)
	serverIdList := make([]int, 0)
	i := 0
	for pId := range participants {
		participatedPartitions[i] = int32(pId)
		serverList := config.GetServerIdListByPartitionId(pId)
		for _, sId := range serverList {
			serverIdList = append(serverIdList, sId)
			dcId := config.GetDataCenterIdByServerId(sId)
			serverDcIds[dcId] = true
		}
		i++
	}

	return participatedPartitions, serverDcIds, serverIdList
}

func (c *Client) reSendWriteData(serverTxnId string, data []*rpc.KeyValueVersion) {
	clientTxnId := c.getTxnIdByServerTxnId(serverTxnId)
	execution := c.txnStore.getCurrentExecution(clientTxnId)
	kvList := make([]*rpc.KeyValue, 0)
	for _, kv := range data {
		tmp := &rpc.KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
		}
		kvList = append(kvList, tmp)
	}
	request := &rpc.WriteDataRequest{
		TxnId:           serverTxnId,
		WriteKeyValList: kvList,
	}
	coordinatorId := c.Config.GetLeaderIdByPartitionId(execution.coordinatorPartitionId)
	sender := NewWriteDataSender(request, coordinatorId, c)
	go sender.Send()
}

func (c *Client) predictOneWayLatency(serverList []int) int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	var max int64 = 0
	for _, sId := range serverList {
		if c.delays[sId] > max {
			max = c.delays[sId]
		}
	}

	return max
}

func (c *Client) estimateArrivalTime(serverList []int) []int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	result := make([]int64, c.Config.GetServerNum())
	for _, sId := range serverList {
		result[sId] = c.delays[sId]
	}
	return result
}
