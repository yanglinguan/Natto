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
		}
	}
	return c.txnStore[txnId]
}

func (c *Coordinator) handleReadAndPrepare(op *ReadAndPrepareOp) {
	txnId := op.request.Txn.TxnId
	twoPCInfo := c.initTwoPCInfoIfNotExist(txnId)

	twoPCInfo.readAndPrepareOp = op

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) handleCommitRequest(op *CommitRequestOp) {
	txnId := op.request.TxnId
	twoPCInfo := c.initTwoPCInfoIfNotExist(txnId)

	twoPCInfo.commitRequest = op

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) handleAbortRequest(op *AbortRequestOp) {
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
		if info.readAndPrepareOp != nil && info.commitRequest != nil {
			log.Infof("txn %v is aborted", info.txnId)
			c.sendToParticipantsAndClient(info)
		}
	} else {
		if info.readAndPrepareOp != nil && info.commitRequest != nil {
			if len(info.readAndPrepareOp.request.Txn.ParticipatedPartitionIds) == len(info.preparedPartition) {
				if c.checkReadKeyVersion(info) {
					info.status = COMMIT
				} else {
					info.status = ABORT
				}
				c.sendToParticipantsAndClient(info)
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
		return
	}
	info.resultSent = true
	coordinatorId := int(info.readAndPrepareOp.request.Txn.CoordPartitionId)
	switch info.status {
	case ABORT:
		info.commitRequest.result = false
		info.commitRequest.wait <- true
		request := &rpc.AbortRequest{
			TxnId:         info.txnId,
			IsCoordinator: true,
		}
		if coordinatorId == c.server.partitionId {
			op := &AbortRequestOp{
				abortRequest:      request,
				request:           nil,
				isFromCoordinator: true,
				sendToCoordinator: false,
			}
			c.server.executor.AbortTxn <- op
		} else {
			for _, pId := range info.readAndPrepareOp.request.Txn.ParticipatedPartitionIds {
				serverId := c.server.config.GetServerIdByPartitionId(int(pId))
				connection := c.server.connections[serverId]
				sender := &AbortRequestSender{
					request:    request,
					timeout:    10,
					connection: connection,
				}
				go sender.Send()
			}
		}
	case COMMIT:
		// unblock the client
		info.commitRequest.result = true
		info.commitRequest.wait <- true
		request := &rpc.CommitRequest{
			TxnId:            info.txnId,
			WriteKeyValList:  info.commitRequest.request.WriteKeyValList,
			IsCoordinator:    true,
			ReadKeyVerList:   info.commitRequest.request.ReadKeyVerList,
			IsReadAnyReplica: false,
		}

		if coordinatorId == c.server.partitionId {
			op := &CommitRequestOp{
				request:   request,
				canCommit: false,
				wait:      make(chan bool, 1),
				result:    false,
			}

			c.server.executor.CommitTxn <- op
		} else {
			for _, pId := range info.readAndPrepareOp.request.Txn.ParticipatedPartitionIds {
				serverId := c.server.config.GetServerIdByPartitionId(int(pId))
				connection := c.server.connections[serverId]
				sender := &CommitRequestSender{
					request:    request,
					timeout:    10,
					connection: connection,
				}
				go sender.Send()
			}

		}

	}
}
