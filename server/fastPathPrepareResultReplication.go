package server

import "Carousel-GTS/rpc"

type FastPathPrepareResultReplication struct {
	txnId string
}

func NewFastPathPrepareResultReplication(txnId string) *FastPathPrepareResultReplication {
	return &FastPathPrepareResultReplication{txnId: txnId}
}

func (o *FastPathPrepareResultReplication) Execute(storage *Storage) {

	fastPrepare := &rpc.FastPrepareResultRequest{
		PrepareResult: storage.txnStore[o.txnId].prepareResultRequest,
		IsLeader:      storage.server.IsLeader(),
		RaftTerm:      storage.server.raftNode.GetRaftTerm(),
	}

	coordinatorPartitionId := storage.txnStore[o.txnId].readAndPrepareRequestOp.GetCoordinatorPartitionId()
	dstServerId := storage.server.config.GetLeaderIdByPartitionId(coordinatorPartitionId)

	sender := NewFastPrepareResultSender(fastPrepare, dstServerId, storage.server)
	go sender.Send()
}
