package client

import "time"

type ReadOnly struct {
	*ReadAndPrepare
}

func NewReadOnly(txnId string, priority bool, readKeyList []string, writeKeyList []string) *ReadOnly {
	op := &ReadOnly{NewReadAndPrepareOp(txnId, priority, readKeyList, writeKeyList)}

	return op
}

func (op *ReadOnly) Execute(client *Client) {
	//partitionSet, participants := op.separatePartition(client)
	//
	//participatedPartitions, serverDcIds, serverList := op.getParticipantPartition(participants, client)

	//t := &rpc.Transaction{
	//	TxnId:                    client.genTxnIdToServer(),
	//	ReadKeyList:              op.readKeyList,
	//	WriteKeyList:             op.writeKeyList,
	//	ParticipatedPartitionIds: participatedPartitions,
	//	CoordPartitionId:         int32(-1), // with read-only optimization, read-only txn does not need send to coord
	//	ReadOnly:                 true,
	//	HighPriority:             op.priority,
	//}

	client.addTxnIfNotExist(op)

	txn := client.getTxn(op.txnId)

	//_, execution := c.getTxnAndExecution(op.txnId)
	maxDelay := client.getMaxDelay(txn.serverIdList, txn.serverDcIds) + time.Now().UnixNano()

	// send read and prepare request to each partition
	for pId, keyLists := range txn.partitionSet {
		request := op.buildRequest(
			keyLists,
			txn.participatedPartitions,
			-1,
			maxDelay,
			txn.participants[pId],
			client)

		// read-only txn only send to partition leader
		partitionLeaderId := client.Config.GetLeaderIdByPartitionId(pId)

		sender := NewReadOnlySender(request, client.getCurrentExecutionCount(op.txnId), op.txnId, partitionLeaderId, client)
		go sender.Send()
	}
}
