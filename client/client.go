package client

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/connection"
	"Carousel-GTS/rpc"
	"fmt"
	"github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

//const QueueLen = 1024

type Transaction struct {
	// when retry, txnId is different from txn.TxnId
	txnId               string
	rpcTxn              *rpc.Transaction
	readAndPrepareReply chan *rpc.ReadAndPrepareReply
	commitReply         chan *rpc.CommitReply
	readKeyValueVersion []*rpc.KeyValueVersion
	commitResult        int
	startTime           time.Time
	endTime             time.Time
	execCount           int
}

type SendOp struct {
	txnId        string
	readKeyList  []string
	writeKeyList []string
	readResult   map[string]string
	wait         chan bool
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
}

func (o *CommitOp) BlockOwner() bool {
	return <-o.wait
}

type Client struct {
	clientId           int
	Config             configuration.Configuration
	clientDataCenterId string

	connections map[string]connection.Connection

	sendTxnRequest   chan *SendOp
	commitTxnRequest chan *CommitOp

	txnStore map[string]*Transaction
	lock     sync.Mutex

	count int
}

func NewClient(clientId int, configFile string) *Client {
	config := configuration.NewFileConfiguration(configFile)
	queueLen := config.GetQueueLen()
	c := &Client{
		clientId:           clientId,
		Config:             config,
		clientDataCenterId: "",
		connections:        make(map[string]connection.Connection),
		sendTxnRequest:     make(chan *SendOp, queueLen),
		commitTxnRequest:   make(chan *CommitOp, queueLen),
		txnStore:           make(map[string]*Transaction),
		lock:               sync.Mutex{},
		count:              0,
	}

	c.clientDataCenterId = c.Config.GetClientDataCenterIdByClientId(clientId)
	if c.Config.GetConnectionPoolSize() == 0 {
		for sId, addr := range c.Config.GetServerAddressMap() {
			c.connections[sId] = connection.NewSingleConnect(addr)
		}
	} else {
		for sId, addr := range c.Config.GetServerAddressMap() {
			c.connections[sId] = connection.NewPoolConnection(addr, c.Config.GetConnectionPoolSize())
		}
	}

	go c.sendReadAndPrepareRequest()
	go c.sendCommitRequest()
	return c
}

func (c *Client) sendReadAndPrepareRequest() {
	for {
		op := <-c.sendTxnRequest
		c.handleReadAndPrepareRequest(op)
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

func (c *Client) ReadAndPrepare(readKeyList []string, writeKeyList []string, txnId string) map[string]string {
	sendOp := &SendOp{
		txnId:        c.getTxnId(txnId),
		readKeyList:  readKeyList,
		writeKeyList: writeKeyList,
		readResult:   make(map[string]string),
		wait:         make(chan bool, 1),
	}

	c.sendTxnRequest <- sendOp

	sendOp.BlockOwner()

	return sendOp.readResult
}

func (c *Client) waitReadAndPrepareRequest(op *SendOp, ongoingTxn *Transaction) {
	// wait for result
	for {
		readAndPrepareRequest := <-ongoingTxn.readAndPrepareReply
		for _, kv := range readAndPrepareRequest.KeyValVerList {
			ongoingTxn.readKeyValueVersion = append(ongoingTxn.readKeyValueVersion, kv)
		}

		if len(ongoingTxn.readKeyValueVersion) == len(ongoingTxn.rpcTxn.ReadKeyList) {
			break
		}
	}

	for _, kv := range ongoingTxn.readKeyValueVersion {
		op.readResult[kv.Key] = kv.Value
	}

	op.wait <- true
}

func (c *Client) addTxn(txn *Transaction) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.txnStore[txn.txnId] = txn
}

func (c *Client) getTxn(txnId string) *Transaction {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.txnStore[txnId]
}

func (c *Client) checkExistAndAddExec(txnId string, rpcTxn *rpc.Transaction) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, exist := c.txnStore[txnId]; exist {
		c.txnStore[txnId].readKeyValueVersion = make([]*rpc.KeyValueVersion, 0)
		c.txnStore[txnId].rpcTxn = rpcTxn
		c.txnStore[txnId].execCount++
		logrus.Infof("RETRY txn %v: %v", txnId, c.txnStore[txnId].execCount)
		return true
	}
	return false
}

func (c *Client) handleReadAndPrepareRequest(op *SendOp) {
	readKeyList := op.readKeyList
	writeKeyList := op.writeKeyList

	// separate key into partitions
	partitionSet := make(map[int][][]string)
	participants := make(map[int]bool)
	for _, key := range readKeyList {
		pId := c.Config.GetPartitionIdByKey(key)
		logrus.Debugf("key %v, pId %v", key, pId)
		if _, exist := partitionSet[pId]; !exist {
			partitionSet[pId] = make([][]string, 2)
		}
		partitionSet[pId][0] = append(partitionSet[pId][0], key)
		participants[pId] = true
	}

	for _, key := range writeKeyList {
		pId := c.Config.GetPartitionIdByKey(key)
		logrus.Debugf("key %v, pId %v", key, pId)
		if _, exist := partitionSet[pId]; !exist {
			partitionSet[pId] = make([][]string, 2)
		}
		partitionSet[pId][1] = append(partitionSet[pId][1], key)
		participants[pId] = true
	}

	participatedPartitions := make([]int32, len(partitionSet))
	serverDcIds := make([]string, len(partitionSet))
	i := 0
	for pId := range partitionSet {
		participatedPartitions[i] = int32(pId)
		sId := c.Config.GetServerIdByPartitionId(pId)
		serverDcIds[i] = c.Config.GetDataCenterIdByServerId(sId)
		i++
	}

	serverList := c.Config.GetServerListByDataCenterId(c.clientDataCenterId)

	coordinatorPartitionId := c.Config.GetPartitionIdByServerId(serverList[rand.Intn(len(serverList))])
	if _, exist := partitionSet[coordinatorPartitionId]; !exist {
		partitionSet[coordinatorPartitionId] = make([][]string, 2)
	}

	t := &rpc.Transaction{
		TxnId:                    c.genTxnIdToServer(),
		ReadKeyList:              readKeyList,
		WriteKeyList:             writeKeyList,
		ParticipatedPartitionIds: participatedPartitions,
		CoordPartitionId:         int32(coordinatorPartitionId),
	}

	if !c.checkExistAndAddExec(op.txnId, t) {
		txn := &Transaction{
			txnId:               op.txnId,
			rpcTxn:              t,
			readAndPrepareReply: make(chan *rpc.ReadAndPrepareReply, len(partitionSet)),
			commitReply:         make(chan *rpc.CommitReply, 1),
			readKeyValueVersion: make([]*rpc.KeyValueVersion, 0),
			commitResult:        0,
			startTime:           time.Now(),
			endTime:             time.Time{},
			execCount:           0,
		}

		c.addTxn(txn)
	}

	ongoingTxn := c.getTxn(op.txnId)

	maxDelay := c.Config.GetMaxDelay(c.clientDataCenterId, serverDcIds).Nanoseconds()
	maxDelay += c.Config.GetDelay().Nanoseconds()
	maxDelay += time.Now().UnixNano()

	// send read and prepare request to each partition
	for pId, keyLists := range partitionSet {
		txn := &rpc.Transaction{
			TxnId:                    ongoingTxn.rpcTxn.TxnId,
			ReadKeyList:              keyLists[0],
			WriteKeyList:             keyLists[1],
			ParticipatedPartitionIds: participatedPartitions,
			CoordPartitionId:         int32(coordinatorPartitionId),
		}

		request := &rpc.ReadAndPrepareRequest{
			Txn:              txn,
			IsRead:           false,
			IsNotParticipant: !participants[pId],
			Timestamp:        0,
			ClientId:         "c" + strconv.Itoa(c.clientId),
		}

		if c.Config.GetServerMode() != configuration.OCC {
			request.Timestamp = maxDelay
		}

		sId := c.Config.GetServerIdByPartitionId(pId)
		sender := NewReadAndPrepareSender(request, ongoingTxn, c.connections[sId])

		go sender.Send()
	}

	go c.waitReadAndPrepareRequest(op, ongoingTxn)
}

func (c *Client) Commit(writeKeyValue map[string]string, txnId string) (bool, bool, time.Duration) {
	commitOp := &CommitOp{
		txnId:         c.getTxnId(txnId),
		writeKeyValue: writeKeyValue,
		wait:          make(chan bool, 1),
	}

	c.commitTxnRequest <- commitOp
	commitOp.BlockOwner()
	return commitOp.result, commitOp.retry, commitOp.waitTime
}

func (c *Client) waitCommitReply(op *CommitOp, ongoingTxn *Transaction) {
	result := <-ongoingTxn.commitReply

	ongoingTxn.endTime = time.Now()
	if result.Result {
		ongoingTxn.commitResult = 1
	} else {
		ongoingTxn.commitResult = 0
	}
	op.result = result.Result
	op.retry, op.waitTime = c.isRetryTxn(ongoingTxn.execCount)
	op.wait <- true
}

func (c *Client) isRetryTxn(execNum int) (bool, time.Duration) {
	if c.Config.GetRetryMode() == configuration.OFF ||
		(c.Config.GetMaxRetry() >= 0 && execNum >= c.Config.GetMaxRetry()) {
		return false, 0
	}

	waitTime := c.Config.GetRetryInterval()

	if c.Config.GetRetryMode() == configuration.EXP {
		//exponential back-off
		abortNum := execNum
		randomFactor := rand.Int63n(int64(math.Exp2(float64(abortNum))))
		if randomFactor > c.Config.GetRetryMaxSlot() {
			randomFactor = c.Config.GetRetryMaxSlot()
		}
		waitTime = c.Config.GetRetryInterval() * time.Duration(randomFactor)
	}
	return true, waitTime
}

func (c *Client) handleCommitRequest(op *CommitOp) {
	writeKeyValue := op.writeKeyValue

	writeKeyValueList := make([]*rpc.KeyValue, len(writeKeyValue))
	i := 0
	for k, v := range writeKeyValue {
		writeKeyValueList[i] = &rpc.KeyValue{
			Key:   k,
			Value: v,
		}
		i++
	}

	ongoingTxn := c.getTxn(op.txnId)

	readKeyVerList := make([]*rpc.KeyVersion, len(ongoingTxn.readKeyValueVersion))
	i = 0
	for _, kv := range ongoingTxn.readKeyValueVersion {
		readKeyVerList[i] = &rpc.KeyVersion{
			Key:     kv.Key,
			Version: kv.Version,
		}
		i++
	}

	request := &rpc.CommitRequest{
		TxnId:            ongoingTxn.rpcTxn.TxnId,
		WriteKeyValList:  writeKeyValueList,
		IsCoordinator:    false,
		ReadKeyVerList:   readKeyVerList,
		IsReadAnyReplica: false,
	}

	coordinatorId := c.Config.GetServerIdByPartitionId(int(ongoingTxn.rpcTxn.CoordPartitionId))
	sender := NewCommitRequestSender(request, ongoingTxn, c.connections[coordinatorId])

	go sender.Send()

	go c.waitCommitReply(op, ongoingTxn)
}

func (c *Client) getCommitTxn() map[int]int {
	commitTxn := make(map[int]int)
	for _, txn := range c.txnStore {
		if txn.commitResult == 1 {
			for _, pId := range txn.rpcTxn.ParticipatedPartitionIds {
				commitTxn[int(pId)]++
			}
		}
	}
	return commitTxn
}

func (c *Client) PrintTxnStatisticData() {
	commitTxn := c.getCommitTxn()
	for sId, conn := range c.connections {
		pId := c.Config.GetPartitionIdByServerId(sId)
		committed := commitTxn[pId]
		request := &rpc.PrintStatusRequest{
			CommittedTxn: int32(committed),
		}
		sender := NewPrintStatusRequestSender(request, conn)
		go sender.Send()
	}

	file, err := os.Create("c" + strconv.Itoa(c.clientId) + "_statistic.log")
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
		key := make([]int, len(txn.rpcTxn.ReadKeyList))
		for i, ks := range txn.rpcTxn.ReadKeyList {
			var k int
			_, err = fmt.Sscan(ks, &k)
			key[i] = k
		}
		s := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v\n",
			txn.txnId,
			txn.commitResult,
			txn.endTime.Sub(txn.startTime).Nanoseconds(),
			txn.startTime.UnixNano(),
			txn.endTime.UnixNano(),
			key,
			txn.execCount)
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
