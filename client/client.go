package client

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/connection"
	"Carousel-GTS/rpc"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const QueueLen = 1024

type Transaction struct {
	txn                 *rpc.Transaction
	readAndPrepareReply chan *rpc.ReadAndPrepareReply
	commitReply         chan *rpc.CommitReply
	readKeyValueVersion []*rpc.KeyValueVersion
	commitResult        int
	startTime           time.Time
	endTime             time.Time
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
}

func (o *CommitOp) BlockOwner() bool {
	return <-o.wait
}

type Client struct {
	clientId           string
	config             configuration.Configuration
	clientDataCenterId string

	connections map[string]connection.Connection

	sendTxnRequest   chan *SendOp
	commitTxnRequest chan *CommitOp
	count            int

	txnStore map[string]*Transaction
	lock     sync.Mutex
}

func NewClient(clientId string, configFile string) *Client {
	c := &Client{
		clientId:           clientId,
		config:             configuration.NewFileConfiguration(configFile),
		clientDataCenterId: "",
		connections:        make(map[string]connection.Connection),
		sendTxnRequest:     make(chan *SendOp, QueueLen),
		commitTxnRequest:   make(chan *CommitOp, QueueLen),
		count:              0,
		txnStore:           make(map[string]*Transaction),
		lock:               sync.Mutex{},
	}

	c.clientDataCenterId = c.config.GetClientDataCenterIdByClientId(clientId)
	if c.config.GetConnectionPoolSize() == 0 {
		for sId, addr := range c.config.GetServerAddressMap() {
			c.connections[sId] = connection.NewSingleConnect(addr)
		}
	} else {
		for sId, addr := range c.config.GetServerAddressMap() {
			c.connections[sId] = connection.NewPoolConnection(addr, c.config.GetConnectionPoolSize())
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

func (c *Client) getTxnId() string {
	return c.clientId + "-" + strconv.Itoa(c.count)
}

func (c *Client) ReadAndPrepare(readKeyList []string, writeKeyList []string) (map[string]string, string) {
	sendOp := &SendOp{
		readKeyList:  readKeyList,
		writeKeyList: writeKeyList,
		readResult:   make(map[string]string),
		wait:         make(chan bool, 1),
	}

	c.sendTxnRequest <- sendOp

	sendOp.BlockOwner()

	return sendOp.readResult, sendOp.txnId
}

func (c *Client) waitReadAndPrepareRequest(op *SendOp, ongoingTxn *Transaction) {
	// wait for result
	for {
		readAndPrepareRequest := <-ongoingTxn.readAndPrepareReply
		for _, kv := range readAndPrepareRequest.KeyValVerList {
			ongoingTxn.readKeyValueVersion = append(ongoingTxn.readKeyValueVersion, kv)
		}

		if len(ongoingTxn.readKeyValueVersion) == len(ongoingTxn.txn.ReadKeyList) {
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
	c.txnStore[txn.txn.TxnId] = txn
}

func (c *Client) getTxn(txnId string) *Transaction {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.txnStore[txnId]
}

func (c *Client) handleReadAndPrepareRequest(op *SendOp) {
	readKeyList := op.readKeyList
	writeKeyList := op.writeKeyList
	c.count++
	txnId := c.getTxnId()
	op.txnId = txnId

	// separate key into partitions
	partitionSet := make(map[int][][]string)
	participants := make(map[int]bool)
	for _, key := range readKeyList {
		pId := c.config.GetPartitionIdByKey(key)
		logrus.Debugf("key %v, pId %v", key, pId)
		if _, exist := partitionSet[pId]; !exist {
			partitionSet[pId] = make([][]string, 2)
		}
		partitionSet[pId][0] = append(partitionSet[pId][0], key)
		participants[pId] = true
	}

	for _, key := range writeKeyList {
		pId := c.config.GetPartitionIdByKey(key)
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
		sId := c.config.GetServerIdByPartitionId(pId)
		serverDcIds[i] = c.config.GetDataCenterIdByServerId(sId)
		i++
	}

	serverList := c.config.GetServerListByDataCenterId(c.clientDataCenterId)

	coordinatorPartitionId := c.config.GetPartitionIdByServerId(serverList[rand.Intn(len(serverList))])
	if _, exist := partitionSet[coordinatorPartitionId]; !exist {
		partitionSet[coordinatorPartitionId] = make([][]string, 2)
	}

	t := &rpc.Transaction{
		TxnId:                    txnId,
		ReadKeyList:              readKeyList,
		WriteKeyList:             writeKeyList,
		ParticipatedPartitionIds: participatedPartitions,
		CoordPartitionId:         int32(coordinatorPartitionId),
	}

	ongoingTxn := &Transaction{
		txn:                 t,
		readAndPrepareReply: make(chan *rpc.ReadAndPrepareReply, len(partitionSet)),
		commitReply:         make(chan *rpc.CommitReply, 1),
		readKeyValueVersion: make([]*rpc.KeyValueVersion, 0),
		commitResult:        0,
		startTime:           time.Now(),
		endTime:             time.Time{},
	}

	c.addTxn(ongoingTxn)

	maxDelay := c.config.GetMaxDelay(c.clientDataCenterId, serverDcIds).Nanoseconds()
	maxDelay += c.config.GetDelay().Nanoseconds()
	maxDelay += time.Now().UnixNano()

	// send read and prepare request to each partition
	for pId, keyLists := range partitionSet {
		txn := &rpc.Transaction{
			TxnId:                    txnId,
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
			ClientId:         c.clientId,
		}

		if c.config.GetServerMode() != configuration.OCC {
			request.Timestamp = maxDelay
		}

		sId := c.config.GetServerIdByPartitionId(pId)
		sender := NewReadAndPrepareSender(request, ongoingTxn, c.connections[sId])

		go sender.Send()
	}

	go c.waitReadAndPrepareRequest(op, ongoingTxn)
}

func (c *Client) Commit(writeKeyValue map[string]string, txnId string) bool {
	commitOp := &CommitOp{
		txnId:         txnId,
		writeKeyValue: writeKeyValue,
		wait:          make(chan bool, 1),
	}

	c.commitTxnRequest <- commitOp
	commitOp.BlockOwner()
	return commitOp.result
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
	op.wait <- true
}

func (c *Client) handleCommitRequest(op *CommitOp) {
	writeKeyValue := op.writeKeyValue
	txnId := op.txnId

	writeKeyValueList := make([]*rpc.KeyValue, len(writeKeyValue))
	i := 0
	for k, v := range writeKeyValue {
		writeKeyValueList[i] = &rpc.KeyValue{
			Key:   k,
			Value: v,
		}
		i++
	}

	ongoingTxn := c.getTxn(txnId)

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
		TxnId:            txnId,
		WriteKeyValList:  writeKeyValueList,
		IsCoordinator:    false,
		ReadKeyVerList:   readKeyVerList,
		IsReadAnyReplica: false,
	}

	coordinatorId := c.config.GetServerIdByPartitionId(int(ongoingTxn.txn.CoordPartitionId))
	sender := NewCommitRequestSender(request, ongoingTxn, c.connections[coordinatorId])

	go sender.Send()

	go c.waitCommitReply(op, ongoingTxn)

}

func (c *Client) PrintTxnStatisticData() {
	for _, conn := range c.connections {
		sender := NewPrintStatusRequestSender(conn)
		sender.Send()
	}

	file, err := os.Create("statistic.log")
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
		key := make([]int, len(txn.txn.ReadKeyList))
		for i, ks := range txn.txn.ReadKeyList {
			var k int
			_, err = fmt.Sscan(ks, &k)
			key[i] = k
		}
		s := fmt.Sprintf("%v,%v,%v, %v, %v, %v\n",
			txn.txn.TxnId,
			txn.commitResult,
			txn.endTime.Sub(txn.startTime).Nanoseconds(),
			txn.startTime.UnixNano(),
			txn.endTime.UnixNano(),
			key)
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

func (c *Client) GetKeyNum() int {
	return c.config.GetKeyNum()
}

func (c *Client) GetKeySize() int {
	return c.config.GetKeySize()
}

func (c *Client) GetTxnSize() int {
	return c.config.GetTxnSize()
}
