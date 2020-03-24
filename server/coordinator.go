package server

import (
	"Carousel-GTS/rpc"
	"bytes"
	"encoding/gob"
	log "github.com/sirupsen/logrus"
)

type TwoPCInfo struct {
	txnId             string
	status            TxnStatus
	preparedPartition map[int]*PrepareResultOp
	readAndPrepareOp  *ReadAndPrepareOp
	commitRequest     *CommitRequestOp
	abortRequest      *AbortRequestOp
	resultSent        bool
	writeData         []*rpc.KeyValue
}

type Coordinator struct {
	txnStore map[string]*TwoPCInfo
	server   *Server

	PrepareResult    chan *PrepareResultOp
	Wait2PCResultTxn chan *ReadAndPrepareOp
	CommitRequest    chan *CommitRequestOp
	AbortRequest     chan *AbortRequestOp
	Replication      chan ReplicationMsg
}

func NewCoordinator(server *Server) *Coordinator {
	queueLen := server.config.GetQueueLen()
	c := &Coordinator{
		txnStore:         make(map[string]*TwoPCInfo),
		server:           server,
		PrepareResult:    make(chan *PrepareResultOp, queueLen),
		Wait2PCResultTxn: make(chan *ReadAndPrepareOp, queueLen),
		CommitRequest:    make(chan *CommitRequestOp, queueLen),
		AbortRequest:     make(chan *AbortRequestOp, queueLen),
		Replication:      make(chan ReplicationMsg, queueLen),
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
		}
	}
}

func (c *Coordinator) initTwoPCInfoIfNotExist(txnId string) *TwoPCInfo {
	if _, exist := c.txnStore[txnId]; !exist {
		c.txnStore[txnId] = &TwoPCInfo{
			txnId:             txnId,
			status:            INIT,
			preparedPartition: make(map[int]*PrepareResultOp),
			resultSent:        false,
		}
	}
	return c.txnStore[txnId]
}

func (c *Coordinator) handleReplicationMsg(msg ReplicationMsg) {
	switch msg.MsgType {
	case CommitResultMsg:
		if c.server.IsLeader() {
			c.sendToParticipantsAndClient(c.txnStore[msg.TxnId])
		} else {
			c.txnStore[msg.TxnId].status = msg.Status
			c.txnStore[msg.TxnId].writeData = msg.WriteData
		}
	}
}

func (c *Coordinator) handleReadAndPrepare(op *ReadAndPrepareOp) {
	log.Debugf("receive read and prepare from client %v", op.request.Txn.TxnId)
	txnId := op.request.Txn.TxnId
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

	c.checkResult(twoPCInfo)
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
	switch twoPCInfo.status {
	case COMMIT:
		log.Fatalln("txn %v should not be prepared without the result from partition %v", txnId, result.Request.PartitionId)
		break
	case ABORT:
		log.Debugf("txn %v is already abort", txnId)
		return
	default:
		break
	}

	if result.Request.PrepareStatus == int32(ABORT) {
		twoPCInfo.status = ABORT
	} else {
		twoPCInfo.preparedPartition[(int)(result.Request.PartitionId)] = result
	}

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) checkReadKeyVersion(info *TwoPCInfo) bool {
	preparedKeyVersion := make(map[string]uint64)
	for _, p := range info.preparedPartition {
		for _, kv := range p.Request.ReadKeyVerList {
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

func (c *Coordinator) convertReplicationMsgToByte(txnId string, msgType ReplicationMsgType) bytes.Buffer {
	replicationMsg := ReplicationMsg{
		TxnId:             txnId,
		Status:            c.txnStore[txnId].status,
		MsgType:           msgType,
		WriteData:         c.txnStore[txnId].commitRequest.request.WriteKeyValList,
		IsFromCoordinator: true,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replicationMsg); err != nil {
		log.Errorf("replication encoding error: %v", err)
	}
	return buf
}

func (c *Coordinator) replicateCommitResult(txnId string) {
	c.txnStore[txnId].resultSent = true
	if !c.server.config.GetReplication() {
		log.Debugf("txn %v config not replication", txnId)
		c.sendToParticipantsAndClient(c.txnStore[txnId])
		return
	}
	buf := c.convertReplicationMsgToByte(txnId, CommitResultMsg)
	c.server.raft.raftInputChannel <- string(buf.Bytes())
}

func (c *Coordinator) checkResult(info *TwoPCInfo) {
	if info.status == ABORT {
		if info.readAndPrepareOp != nil && !info.resultSent {
			log.Infof("txn %v is aborted", info.txnId)
			c.replicateCommitResult(info.txnId)
			//c.sendToParticipantsAndClient(info)
		}
	} else {
		if info.readAndPrepareOp != nil && info.commitRequest != nil && !info.resultSent {
			if len(info.readAndPrepareOp.request.Txn.ParticipatedPartitionIds) == len(info.preparedPartition) && info.status == INIT {
				if c.checkReadKeyVersion(info) {
					log.Debugf("txn %v commit coordinator %v", info.txnId, info.preparedPartition)
					info.status = COMMIT
				} else {
					log.Debugf("txn %v abort coordinator %v, due to read version invalid",
						info.txnId, info.preparedPartition)
					info.status = ABORT
				}
				c.replicateCommitResult(info.txnId)
				//c.sendToParticipantsAndClient(info)
			} else {
				log.Debugf("txn %v cannot commit yet required partitions %v, received partition %v, status %v",
					info.readAndPrepareOp.request.Txn.ParticipatedPartitionIds,
					len(info.preparedPartition),
					info.status)
			}
		} else {
			log.Debugf("txn %v cannot commit yet, read and prepared request or commit request does not received from client already sent %v", info.txnId, info.resultSent)
		}
	}
}

func (c *Coordinator) sendToParticipantsAndClient(info *TwoPCInfo) {
	//info.resultSent = true
	//c.replicateCommitResult(info.txnId)
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
				serverId := c.server.config.GetServerIdByPartitionId(int(pId))
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
				TxnId:            info.txnId,
				WriteKeyValList:  partitionWriteKV[int(pId)],
				IsCoordinator:    true,
				ReadKeyVerList:   partitionReadVersion[int(pId)],
				IsReadAnyReplica: false,
			}
			if int(pId) == c.server.partitionId {
				op := NewCommitRequestOp(request)
				c.server.executor.CommitTxn <- op
			} else {
				log.Debugf("send to commit to pId %v, txn %v", pId, request.TxnId)
				serverId := c.server.config.GetServerIdByPartitionId(int(pId))
				sender := NewCommitRequestSender(request, serverId, c.server)
				go sender.Send()
			}
		}
		break
	}
}
