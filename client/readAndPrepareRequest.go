package client

import "C"
import (
	"Carousel-GTS/rpc"
	"github.com/sirupsen/logrus"
	"math/rand"
	"strconv"
)

type ReadAndPrepare struct {
	txnId        string
	readKeyList  []string
	writeKeyList []string
	readResult   map[string]string
	isAbort      bool
	wait         chan bool
	priority     bool
}

func NewReadAndPrepareOp(txnId string, priority bool, readKeyList []string, writeKeyList []string) *ReadAndPrepare {
	r := &ReadAndPrepare{
		txnId:        txnId,
		readKeyList:  readKeyList,
		writeKeyList: writeKeyList,
		readResult:   make(map[string]string),
		isAbort:      false,
		wait:         make(chan bool, 1),
		priority:     priority,
	}

	return r
}

func (op *ReadAndPrepare) Execute(client *Client) {
	client.addTxnIfNotExist(op)

	txn := client.getTxn(op.txnId)
	coordinatorPartitionId := op.findCoordinatorPartitionId(txn.participants, client)
	isCoordinatorPartition := true
	if _, exist := txn.partitionSet[coordinatorPartitionId]; !exist {
		isCoordinatorPartition = false
		txn.partitionSet[coordinatorPartitionId] = make([][]string, 2)
	}

	client.getCurrentExecution(op.txnId).setCoordinatorPartitionId(coordinatorPartitionId)

	maxDelay := client.getMaxDelay(txn.serverIdList, txn.serverDcIds)

	// send read and prepare request to each partition
	for pId, keyLists := range txn.partitionSet {
		request := op.buildRequest(
			keyLists,
			txn.participatedPartitions,
			coordinatorPartitionId,
			maxDelay,
			txn.participants[pId],
			client)

		if request.IsNotParticipant || (request.Txn.ReadOnly && client.Config.GetIsReadOnly()) ||
			!client.Config.GetFastPath() {
			// only send to the leader of non-participant partition
			sId := client.Config.GetLeaderIdByPartitionId(pId)
			sender := NewReadAndPrepareSender(request, client.getCurrentExecutionCount(op.txnId), op.txnId, sId, client)
			go sender.Send()
		} else {
			sIdList := client.Config.GetServerIdListByPartitionId(pId)
			for _, sId := range sIdList {
				sender := NewReadAndPrepareSender(request, client.getCurrentExecutionCount(op.txnId), op.txnId, sId, client)
				go sender.Send()
			}
		}
	}

	// if the coordinator is not participant partition, remove it for retry
	if !isCoordinatorPartition {
		delete(txn.partitionSet, coordinatorPartitionId)
	}
}

func (op *ReadAndPrepare) buildRequest(
	keyLists [][]string,
	participatedPartitions []int32,
	coordinatorPartitionId int,
	maxDelay int64,
	isParticipants bool,
	client *Client) *rpc.ReadAndPrepareRequest {

	txn := &rpc.Transaction{
		TxnId:                    client.getCurrentExecutionTxnId(op.txnId),
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
		IsNotParticipant: !isParticipants,
		Timestamp:        maxDelay,
		ClientId:         "c" + strconv.Itoa(client.clientId),
	}

	return request

}

func (op *ReadAndPrepare) findCoordinatorPartitionId(participants map[int]bool, client *Client) int {
	// find coordinator
	leaderIdList := client.Config.GetLeaderIdListByDataCenterId(client.clientDataCenterId)
	coordinatorPartitionId := client.Config.GetPartitionIdByServerId(leaderIdList[rand.Intn(len(leaderIdList))])
	logrus.Debugf("txn %v client datacenterId %v local leader %v coordinatorId %v",
		op.txnId, client.clientDataCenterId, leaderIdList, coordinatorPartitionId)
	for _, lId := range leaderIdList {
		pLId := client.Config.GetPartitionIdByServerId(lId)
		if _, exist := participants[pLId]; exist {
			coordinatorPartitionId = pLId
			break
		}
	}

	return coordinatorPartitionId
}

func (op *ReadAndPrepare) Block() {
	<-op.wait
}

func (op *ReadAndPrepare) Unblock() {
	op.wait <- true
}

func (op *ReadAndPrepare) GetReadResult() map[string]string {
	return op.readResult
}

func (op *ReadAndPrepare) IsAbort() bool {
	return op.isAbort
}

func (op *ReadAndPrepare) GetTxnId() string {
	return op.txnId
}

func (op *ReadAndPrepare) GetReadKeyList() []string {
	return op.readKeyList
}

func (op *ReadAndPrepare) GetWriteKeyList() []string {
	return op.writeKeyList
}

func (op *ReadAndPrepare) ClearReadKeyList() {
	op.readKeyList = nil
}

func (op *ReadAndPrepare) ClearWriteKeyList() {
	op.writeKeyList = nil
}

func (op *ReadAndPrepare) GetPriority() bool {
	return op.priority
}

func (op *ReadAndPrepare) SetKeyValue(key, value string) {
	op.readResult[key] = value
}

func (op *ReadAndPrepare) SetAbort(abort bool) {
	op.isAbort = abort
}
