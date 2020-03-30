package server

import (
	"Carousel-GTS/rpc"
	"bytes"
	"encoding/gob"
	log "github.com/sirupsen/logrus"
)

type PartitionStatus struct {
	status        TxnStatus
	isFastPrepare bool
	prepareResult *rpc.PrepareResultRequest
}

func NewPartitionStatus(status TxnStatus, isFastPrepare bool, prepareResult *rpc.PrepareResultRequest) *PartitionStatus {
	p := &PartitionStatus{
		status:        status,
		isFastPrepare: isFastPrepare,
		prepareResult: prepareResult,
	}
	return p
}

type TwoPCInfo struct {
	txnId               string
	status              TxnStatus
	readAndPrepareOp    *ReadAndPrepareOp
	commitRequest       *CommitRequestOp
	abortRequest        *AbortRequestOp
	resultSent          bool
	writeData           []*rpc.KeyValue
	writeDataReplicated bool
	//slowPathPreparePartition map[int]*PrepareResultOp
	fastPathPreparePartition map[int]*FastPrepareStatus
	partitionPrepareResult   map[int]*PartitionStatus
}

type Coordinator struct {
	txnStore map[string]*TwoPCInfo
	server   *Server

	PrepareResult     chan *PrepareResultOp
	Wait2PCResultTxn  chan *ReadAndPrepareOp
	CommitRequest     chan *CommitRequestOp
	AbortRequest      chan *AbortRequestOp
	Replication       chan ReplicationMsg
	FastPrepareResult chan *FastPrepareResultOp
}

func NewCoordinator(server *Server) *Coordinator {
	queueLen := server.config.GetQueueLen()
	c := &Coordinator{
		txnStore:          make(map[string]*TwoPCInfo),
		server:            server,
		PrepareResult:     make(chan *PrepareResultOp, queueLen),
		Wait2PCResultTxn:  make(chan *ReadAndPrepareOp, queueLen),
		CommitRequest:     make(chan *CommitRequestOp, queueLen),
		AbortRequest:      make(chan *AbortRequestOp, queueLen),
		Replication:       make(chan ReplicationMsg, queueLen),
		FastPrepareResult: make(chan *FastPrepareResultOp, queueLen),
	}

	go c.run()

	return c
}

func (c *Coordinator) run() {
	for {
		select {
		case result := <-c.PrepareResult:
			c.handlePrepareResult(result)
		case txn := <-c.Wait2PCResultTxn:
			c.handleReadAndPrepare(txn)
		case com := <-c.CommitRequest:
			c.handleCommitRequest(com)
		case abort := <-c.AbortRequest:
			c.handleAbortRequest(abort)
		case replicationMsg := <-c.Replication:
			c.handleReplicationMsg(replicationMsg)
		case fastPrepare := <-c.FastPrepareResult:
			c.handleFastPrepareResult(fastPrepare)
		}
	}
}

func (c *Coordinator) initTwoPCInfoIfNotExist(txnId string) *TwoPCInfo {
	if _, exist := c.txnStore[txnId]; !exist {
		c.txnStore[txnId] = &TwoPCInfo{
			txnId:                    txnId,
			status:                   INIT,
			resultSent:               false,
			writeDataReplicated:      false,
			fastPathPreparePartition: make(map[int]*FastPrepareStatus),
			partitionPrepareResult:   make(map[int]*PartitionStatus),
		}
	}
	return c.txnStore[txnId]
}

func (c *Coordinator) handleReplicationMsg(msg ReplicationMsg) {
	switch msg.MsgType {
	case WriteDataMsg:
		log.Debugf("server")
		if c.server.IsLeader() {
			c.txnStore[msg.TxnId].writeDataReplicated = true
			if c.txnStore[msg.TxnId].status == COMMIT {
				c.sendToParticipantsAndClient(c.txnStore[msg.TxnId])
			}
		} else {
			c.initTwoPCInfoIfNotExist(msg.TxnId)
			c.txnStore[msg.TxnId].writeData = msg.WriteData
		}
	}
}

func (c *Coordinator) handleReadAndPrepare(op *ReadAndPrepareOp) {
	log.Debugf("receive read and prepare from client %v", op.txnId)
	txnId := op.txnId
	twoPCInfo := c.initTwoPCInfoIfNotExist(txnId)

	twoPCInfo.readAndPrepareOp = op

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) handleCommitRequest(op *CommitRequestOp) {
	log.Debugf("receive commit from client %v", op.request.TxnId)

	txnId := op.request.TxnId
	twoPCInfo := c.initTwoPCInfoIfNotExist(txnId)

	twoPCInfo.commitRequest = op

	if twoPCInfo.status == ABORT {
		log.Infof("TXN %v already aborted", txnId)
		op.result = false
		op.wait <- true
		return
	}

	c.replicateWriteData(txnId)

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) replicateWriteData(txnId string) {
	if !c.server.config.GetReplication() {
		c.txnStore[txnId].writeDataReplicated = true
		log.Debugf("txn %v config not replication", txnId)
		return
	}

	replicationMsg := ReplicationMsg{
		TxnId:             txnId,
		Status:            c.txnStore[txnId].status,
		MsgType:           WriteDataMsg,
		WriteData:         c.txnStore[txnId].commitRequest.request.WriteKeyValList,
		IsFromCoordinator: true,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replicationMsg); err != nil {
		log.Errorf("replication encoding error: %v", err)
	}

	c.server.raft.raftInputChannel <- string(buf.Bytes())
}

func (c *Coordinator) handleAbortRequest(op *AbortRequestOp) {
	log.Debugf("receive abort from client txn %v", op.abortRequest.TxnId)
	txnId := op.abortRequest.TxnId
	twoPCInfo := c.initTwoPCInfoIfNotExist(txnId)

	twoPCInfo.abortRequest = op
	twoPCInfo.status = ABORT

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) handlePrepareResult(result *PrepareResultOp) {
	txnId := result.Request.TxnId
	twoPCInfo := c.initTwoPCInfoIfNotExist(txnId)

	log.Debugf("txn %v receive prepared result from partition %v result %v",
		txnId, result.Request.PartitionId, result.Request.PrepareStatus)
	if !c.server.config.GetFastPath() && twoPCInfo.status == COMMIT {
		log.Fatalf("txn %v cannot commit without the result from partition %v", txnId, result.Request.PartitionId)
		return
	}

	if twoPCInfo.status == ABORT {
		log.Debugf("txn %v is already abort", txnId)
		return
	}

	pId := int(result.Request.PartitionId)
	if _, exist := twoPCInfo.partitionPrepareResult[pId]; exist {
		log.Debugf("txn %v partition %v has prepared result %v, isFastPrepare %v",
			txnId, pId, twoPCInfo.partitionPrepareResult[pId].status, twoPCInfo.partitionPrepareResult[pId].isFastPrepare)
		return
	}

	if TxnStatus(result.Request.PrepareStatus) == ABORT {
		twoPCInfo.status = ABORT
	} else {
		twoPCInfo.partitionPrepareResult[pId] = NewPartitionStatus(
			TxnStatus(result.Request.PrepareStatus), false, result.Request)
	}

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) handleFastPrepareResult(result *FastPrepareResultOp) {
	txnId := result.request.PrepareResult.TxnId
	twoPCInfo := c.initTwoPCInfoIfNotExist(txnId)
	if twoPCInfo.status == ABORT {
		log.Debugf("txn %v is already abort", txnId)
		return
	}

	pId := int(result.request.PrepareResult.PartitionId)
	if _, exist := twoPCInfo.partitionPrepareResult[pId]; exist {
		log.Debugf("txn %v partition %v has prepared result %v, isFastPrepare %v",
			txnId, pId, twoPCInfo.partitionPrepareResult[pId].status, twoPCInfo.partitionPrepareResult[pId].isFastPrepare)
		return
	}

	if _, exist := twoPCInfo.fastPathPreparePartition[pId]; !exist {
		twoPCInfo.fastPathPreparePartition[pId] = NewFastPrepareStatus(txnId, pId, c.server.config.GetSuperMajority())
	}

	twoPCInfo.fastPathPreparePartition[pId].addFastPrepare(result)

	ok, status := twoPCInfo.fastPathPreparePartition[pId].isFastPathSuccess()

	if !ok {
		log.Debugf("txn %v pId %v fast path not success yet", txnId, pId)
		return
	}
	log.Debugf("txn %v pId %v fast path success", txnId, pId)
	if status == ABORT {
		twoPCInfo.status = ABORT
	} else {
		twoPCInfo.partitionPrepareResult[pId] = NewPartitionStatus(status, true, result.request.PrepareResult)
	}

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) checkReadKeyVersion(info *TwoPCInfo) bool {
	preparedKeyVersion := make(map[string]uint64)
	for _, p := range info.partitionPrepareResult {
		for _, kv := range p.prepareResult.ReadKeyVerList {
			preparedKeyVersion[kv.Key] = kv.Version
		}
	}

	for _, kv := range info.commitRequest.request.ReadKeyVerList {
		if kv.Version != preparedKeyVersion[kv.Key] {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkResult(info *TwoPCInfo) {
	if info.status == ABORT {
		if info.readAndPrepareOp != nil {
			log.Infof("txn %v is aborted", info.txnId)
			c.sendToParticipantsAndClient(info)
		}
	} else {
		if info.status == INIT && info.readAndPrepareOp != nil && info.commitRequest != nil &&
			len(info.readAndPrepareOp.request.Txn.ParticipatedPartitionIds) == len(info.partitionPrepareResult) {
			if c.checkReadKeyVersion(info) {
				log.Debugf("txn %v commit coordinator after check version", info.txnId)
				info.status = COMMIT
				if info.writeDataReplicated {
					c.sendToParticipantsAndClient(info)
				}
			} else {
				log.Debugf("txn %v abort coordinator, due to read version invalid",
					info.txnId)
				info.status = ABORT

				c.sendToParticipantsAndClient(info)
			}
		}
	}
}

func (c *Coordinator) sendToParticipantsAndClient(info *TwoPCInfo) {
	if info.resultSent {
		log.Warnf("txn %v result is sent", info.txnId)
		return
	}
	info.resultSent = true
	switch info.status {
	case ABORT:
		if info.commitRequest != nil {
			info.commitRequest.result = false
			info.commitRequest.wait <- true
		}
		request := &rpc.AbortRequest{
			TxnId:         info.txnId,
			IsCoordinator: true,
		}

		for _, pId := range info.readAndPrepareOp.request.Txn.ParticipatedPartitionIds {
			if int(pId) == c.server.partitionId {
				op := NewAbortRequestOp(request, nil, true)
				c.server.executor.AbortTxn <- op
			} else {
				serverId := c.server.config.GetLeaderIdByPartitionId(int(pId))
				sender := NewAbortRequestSender(request, serverId, c.server)
				go sender.Send()
			}
		}
		break
	case COMMIT:
		// unblock the client
		info.commitRequest.result = true
		info.commitRequest.wait <- true
		partitionWriteKV := make(map[int][]*rpc.KeyValue)
		for _, kv := range info.commitRequest.request.WriteKeyValList {
			pId := c.server.config.GetPartitionIdByKey(kv.Key)
			if _, exist := partitionWriteKV[pId]; !exist {
				partitionWriteKV[pId] = make([]*rpc.KeyValue, 0)
			}
			partitionWriteKV[pId] = append(partitionWriteKV[pId], kv)
		}
		partitionReadVersion := make(map[int][]*rpc.KeyVersion)
		for _, kv := range info.commitRequest.request.ReadKeyVerList {
			pId := c.server.config.GetPartitionIdByKey(kv.Key)
			if _, exist := partitionReadVersion[pId]; !exist {
				partitionReadVersion[pId] = make([]*rpc.KeyVersion, 0)
			}
			partitionReadVersion[pId] = append(partitionReadVersion[pId], kv)
		}

		for _, pId := range info.readAndPrepareOp.request.Txn.ParticipatedPartitionIds {
			request := &rpc.CommitRequest{
				TxnId:             info.txnId,
				WriteKeyValList:   partitionWriteKV[int(pId)],
				IsCoordinator:     true,
				ReadKeyVerList:    partitionReadVersion[int(pId)],
				IsReadAnyReplica:  false,
				IsFastPathSuccess: info.partitionPrepareResult[int(pId)].isFastPrepare,
			}
			if int(pId) == c.server.partitionId {
				op := NewCommitRequestOp(request)
				c.server.executor.CommitTxn <- op
			} else {
				log.Debugf("send to commit to pId %v, txn %v", pId, request.TxnId)
				serverId := c.server.config.GetLeaderIdByPartitionId(int(pId))
				sender := NewCommitRequestSender(request, serverId, c.server)
				go sender.Send()
			}
		}
		break
	}
}
