package server

import (
	"Carousel-GTS/rpc"
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
}

type Coordinator struct {
	txnStore map[string]*TwoPCInfo
	server   *Server

	PrepareResult    chan *PrepareResultOp
	Wait2PCResultTxn chan *ReadAndPrepareOp
	CommitRequest    chan *CommitRequestOp
	AbortRequest     chan *AbortRequestOp
}

func NewCoordinator(server *Server) *Coordinator {
	c := &Coordinator{
		txnStore:         make(map[string]*TwoPCInfo),
		server:           server,
		PrepareResult:    make(chan *PrepareResultOp, QueueLen),
		Wait2PCResultTxn: make(chan *ReadAndPrepareOp, QueueLen),
		CommitRequest:    make(chan *CommitRequestOp, QueueLen),
		AbortRequest:     make(chan *AbortRequestOp, QueueLen),
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

	logger := log.WithFields(log.Fields{
		"txnId":             txnId,
		"txnStatus":         twoPCInfo.status,
		"preparedPartition": twoPCInfo.preparedPartition,
		"partitionId":       result.Request.PartitionId,
	})
	logger.Debugf("receive prepared result")
	switch twoPCInfo.status {
	case COMMIT:
		logger.Fatalln("should not be prepared without the result from this partition")
		break
	case ABORT:
		logger.Debugln("txn is already abort")
		return
	}

	if result.Request.PrepareStatus == ABORT {
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

func (c *Coordinator) checkResult(info *TwoPCInfo) {
	if info.status == ABORT {
		//if info.readAndPrepareOp != nil && info.commitRequest != nil {
		if info.readAndPrepareOp != nil {
			log.Infof("txn %v is aborted", info.txnId)
			c.sendToParticipantsAndClient(info)
		}
	} else {
		if info.readAndPrepareOp != nil && info.commitRequest != nil {
			if len(info.readAndPrepareOp.request.Txn.ParticipatedPartitionIds) == len(info.preparedPartition) {
				if c.checkReadKeyVersion(info) {
					log.Debugf("txn %v commit coordinator %v", info.txnId, info.preparedPartition)
					info.status = COMMIT
				} else {
					info.status = ABORT
				}
				c.sendToParticipantsAndClient(info)
			} else {
				log.WithFields(log.Fields{
					"txnId":              info.txnId,
					"received Partition": len(info.preparedPartition),
					"required":           len(info.readAndPrepareOp.request.Txn.ParticipatedPartitionIds),
				}).Debugln("cannot commit yet")
			}
		} else {
			log.WithFields(log.Fields{
				"txnId":              info.txnId,
				"received Partition": len(info.preparedPartition),
			}).Debugln("cannot commit yet")
		}
	}
}

func (c *Coordinator) sendToParticipantsAndClient(info *TwoPCInfo) {
	if info.resultSent {
		log.Debugf("txn %v result is already sent")
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
				serverId := c.server.config.GetServerIdByPartitionId(int(pId))
				connection := c.server.connections[serverId]
				sender := NewAbortRequestSender(request, connection)
				go sender.Send()
			}
		}
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
				connection := c.server.connections[serverId]
				sender := NewCommitRequestSender(request, connection)
				go sender.Send()
			}
		}
	}
}
