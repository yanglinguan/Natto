package server

import (
	"Carousel-GTS/latencyPredictor"
	"Carousel-GTS/rpc"
	"Carousel-GTS/utils"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

type TwoPCInfo struct {
	txnId           string
	status          TxnStatus
	readRequest     *rpc.ReadAndPrepareRequest
	commitRequestOp *CommitCoordinator
	abortRequest    *rpc.AbortRequest
	resultSent      bool
	//writeData           []*rpc.KeyValue
	writeDataMap        map[string]*rpc.KeyValue
	writeDataFromLeader map[string]bool
	writeDataReceived   bool
	writeDataReplicated bool

	fastPathPreparePartition map[int]*FastPrepareStatus
	partitionPrepareResult   map[int]*PartitionStatus

	conditionGraph *utils.DepGraph
	fastPrepare    bool
	hasCondition   bool
	hasForward     bool

	forwardReadOp *ForwardReadRequestToCoordinator
	//abortReason    AbortReason

	reorderPrepare         bool
	conditionPrepare       bool
	reversedReorderPrepare bool
	reversedReorder        bool
	rePrepare              bool

	forwardPrepare bool
	// this txn commit if and only if dependTxns commit
	dependTxns map[string]bool
	// when this txn commit or abort, it should notify the txns' coordinator
	notifyTxns map[string]int
	// the txns that waiting the result of this txn
	waitingTxns map[string]bool
	// the txns that waiting the write data of this txn
	// keyList
	waitingWriteDataTxn map[string]*forwardReadWaiting
}

type forwardReadWaiting struct {
	keyList  []string
	clientId string
}

func (c *Coordinator) sendReadResultToClient(info *TwoPCInfo, txnId string, clientId string, keys []string) {
	log.Debugf("txn %v 's coord %v send txn %v read keys %v to client %v",
		info.txnId, c.server.serverId, txnId, keys, clientId)
	stream := c.clientReadRequestToCoordinator[clientId]
	readResult := &rpc.ReadReplyFromCoordinator{
		KeyValVerList: make([]*rpc.KeyValueVersion, 0),
		TxnId:         txnId,
	}
	for _, key := range keys {
		value := info.writeDataMap[key].Value
		r := &rpc.KeyValueVersion{
			Key:     key,
			Value:   value,
			Version: uint64(utils.ConvertToInt(value)),
		}
		readResult.KeyValVerList = append(readResult.KeyValVerList, r)
	}

	err := stream.Send(readResult)
	if err != nil {
		log.Fatalf("stream send read result error %v txn %v ", err, txnId)
	}
}

type Coordinator struct {
	txnStore map[string]*TwoPCInfo
	server   *Server

	operation chan CoordinatorOperation

	clientReadRequestToCoordinator map[string]rpc.Carousel_ReadResultFromCoordinatorServer
	latencyPredictor               *latencyPredictor.LatencyPredictor
	probeC                         chan *LatInfo
	probeTimeC                     chan *LatTimeInfo
}

func NewCoordinator(server *Server) *Coordinator {
	queueLen := server.config.GetQueueLen()
	c := &Coordinator{
		txnStore:                       make(map[string]*TwoPCInfo),
		server:                         server,
		operation:                      make(chan CoordinatorOperation, queueLen),
		clientReadRequestToCoordinator: make(map[string]rpc.Carousel_ReadResultFromCoordinatorServer),
	}

	go c.executeOperations()

	return c
}

func (c *Coordinator) startProbe() {
	if c.server.config.UseNetworkTimestamp() &&
		c.server.config.GetFastPath() &&
		c.server.config.IsFastCommit() {
		queueLen := c.server.config.GetQueueLen()
		c.latencyPredictor = latencyPredictor.NewLatencyPredictor(
			c.server.config.GetServerAddress(),
			c.server.config.GetProbeWindowLen(),
			c.server.config.GetProbeWindowMinSize())
		if c.server.config.IsProbeTime() {
			c.probeTimeC = make(chan *LatTimeInfo, queueLen)
		} else {
			c.probeC = make(chan *LatInfo, queueLen)
		}
		if c.server.config.IsProbeTime() {
			go c.probingTime()
			go c.processProbeTime()
		} else {
			go c.probing()
			go c.processProbe()
		}
	}
}

func (c *Coordinator) executeOperations() {
	for {
		op := <-c.operation
		op.Execute(c)
	}
}

func (c *Coordinator) AddOperation(o CoordinatorOperation) {
	c.operation <- o
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
			writeDataMap:             make(map[string]*rpc.KeyValue),
			writeDataFromLeader:      make(map[string]bool),
			conditionGraph:           utils.NewDepGraph(c.server.config.GetTotalPartition()),
			dependTxns:               make(map[string]bool),
			waitingTxns:              make(map[string]bool),
			waitingWriteDataTxn:      make(map[string]*forwardReadWaiting),
			notifyTxns:               make(map[string]int),
		}
	}
	return c.txnStore[txnId]
}

func (c *Coordinator) checkReadKeyVersion(info *TwoPCInfo) bool {
	if len(info.commitRequestOp.request.ReadKeyVerList) == 0 {
		return true
	}

	preparedKeyVersion := make(map[string]uint64)
	for _, p := range info.partitionPrepareResult {
		for _, kv := range p.prepareResult.ReadKeyVerList {
			preparedKeyVersion[kv.Key] = kv.Version
		}
	}

	for _, kv := range info.commitRequestOp.request.ReadKeyVerList {
		if kv.Version != preparedKeyVersion[kv.Key] {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkResult(info *TwoPCInfo) {
	log.Debugf("txn %v check result status %v", info.txnId, info.status)
	if info.status.IsAbort() {
		if info.readRequest != nil {
			log.Infof("txn %v is aborted", info.txnId)
			c.sendToParticipantsAndClient(info)
		}
	} else {
		if info.status == INIT {
			if info.readRequest == nil {
				log.Debugf("txn %v does not receive the read and prepare prepareResult from client", info.txnId)
				return
			}
			if len(info.readRequest.Txn.ParticipatedPartitionIds) != len(info.partitionPrepareResult) {
				log.Debugf("txn %v does not receive all partitions prepare result", info.txnId)
				return
			}
			if info.readRequest.Txn.HighPriority && c.server.config.IsConditionalPrepare() {
				if info.conditionGraph.IsCyclic() {
					log.Warnf("txn %v condition has cycle, condition abort", info.txnId)
					//info.status = CONDITION_ABORT
					//info.abortReason = CYCLE
					//c.sendToParticipantsAndClient(info)
					return
				}
				log.Debugf("txn %v no cycle detected", info.txnId)
			}
			if info.readRequest.Txn.HighPriority && c.server.config.ForwardReadToCoord() {
				satisfyRequest := true
				for depTxn := range info.dependTxns {
					twoPCInfo := c.initTwoPCInfoIfNotExist(depTxn)
					if twoPCInfo.status == INIT {
						log.Debugf("txn %v depTxn %v status is INIT wait the result", info.txnId, depTxn)
						satisfyRequest = false
						twoPCInfo.waitingTxns[info.txnId] = true
					} else if twoPCInfo.status.IsAbort() {
						keys := make(map[string]bool)
						log.Debugf("txn %v depTxn %v is abort not handle for now", info.txnId, depTxn)
						// check keys
						for i, txnId := range info.forwardReadOp.request.ParentTxns {
							if txnId != depTxn {
								continue
							}
							num := int(info.forwardReadOp.request.Idx[i])
							for j := 0; j < num; j++ {
								idx := i + j
								k := info.forwardReadOp.request.KeyList[idx]
								keys[k] = true
							}
							break
						}
						for k := range keys {
							fromLeader, exist := info.writeDataFromLeader[k]
							if exist && fromLeader {
								continue
							}
							log.Debugf("txn %v depend txn %v is abort key %v exist %v",
								info.txnId, depTxn, k, exist)
							satisfyRequest = false
							break
						}
					}
				}
				if !satisfyRequest {
					return
				}
			}
			if info.readRequest.Txn.ReadOnly && c.server.config.GetIsReadOnly() {
				// if this is read only txn, it is prepared we do not need to check version
				// we also do not need to clientWait client commit prepareResult
				info.status = COMMIT
				c.sendToParticipantsAndClient(info)
				return
			}
			// other wise this is read write txn, we need to clientWait client commit prepareResult
			if info.commitRequestOp == nil {
				log.Debugf("txn %v does not receive commit prepareResult from client", info.txnId)
				return
			}

			// when the commit prepareResult is received and all partition results are prepared
			// if client read from any replica we need to check the prepared version. if it reads from leader
			// then we do not need to check the version
			//if info.commitRequestOp.request.IsReadAnyReplica {
			log.Debugf("txn %v need to check version", info.txnId)
			if !c.checkReadKeyVersion(info) {
				log.Debugf("txn %v version check fail %v", info.txnId)
				info.status = READ_VERSION_ABORT
				c.sendToParticipantsAndClient(info)
				return
			}
			//}

			log.Debugf("txn %v can commit replicate data %v", info.txnId, info.status)
			info.status = COMMIT
			//info.fastPrepare = true
			//for _, p := range info.partitionPrepareResult {
			//	info.fastPrepare = info.fastPrepare && p.isFastPrepare
			//}
			if info.writeDataReplicated {
				c.sendToParticipantsAndClient(info)
			}
		}
	}
}

func (c *Coordinator) sendAbort(info *TwoPCInfo) {
	if info.commitRequestOp != nil {
		info.commitRequestOp.result = false
		info.commitRequestOp.unblockClient()
	}

	// with read only optimization, coordinator do not need to send to participant
	if info.readRequest.Txn.ReadOnly && c.server.config.GetIsReadOnly() {
		return
	}

	request := &rpc.AbortRequest{
		TxnId:           info.txnId,
		FromCoordinator: true,
	}

	for _, pId := range info.readRequest.Txn.ParticipatedPartitionIds {
		if c.server.config.GetFastPath() &&
			c.server.config.UseNetworkTimestamp() &&
			c.server.config.IsFastCommit() {

			serverIdList := c.server.config.GetServerIdListByPartitionId(int(pId))
			maxDelay := c.predictOneWayLatency(serverIdList) * 1000000 // change to nanoseconds
			maxDelay += c.server.config.GetDelay().Nanoseconds()
			maxDelay += time.Now().UnixNano()

			fastRequest := &rpc.FastAbortRequest{
				AbortRequest: request,
				Timestamp:    maxDelay,
			}
			for _, serverId := range serverIdList {
				sender := NewFastAbortRequestSender(fastRequest, serverId, c.server)
				go sender.Send()
			}
		} else {
			serverId := c.server.config.GetLeaderIdByPartitionId(int(pId))
			sender := NewAbortRequestSender(request, serverId, c.server)
			go sender.Send()
		}
	}

	c.sendResultToCoordinator(info)
}

func (c *Coordinator) sendResultToCoordinator(info *TwoPCInfo) {
	for txn, dstId := range info.notifyTxns {
		request := &rpc.CommitResult{
			TxnId:  info.txnId,
			Result: info.status == COMMIT,
		}
		log.Debugf("txn %v send commit result %v to server %v for txn %v ", info.txnId, request.Result, txn)
		sender := NewCommitResultToCoordinatorSender(request, dstId, c.server)
		go sender.Send()
	}
}

func (c *Coordinator) sendCommit(info *TwoPCInfo) {
	// unblock the client
	log.Debugf("txn %v coordinator send commit to client", info.txnId)
	if info.commitRequestOp != nil {
		info.commitRequestOp.result = true
		info.commitRequestOp.unblockClient()
	}
	// if it is read only txn and optimization is enabled, coordinator only to reply the result to client
	// do not need to send to partitions, because partitions does not hold the lock of the keys
	if info.readRequest.Txn.ReadOnly && c.server.config.GetIsReadOnly() {
		log.Debugf("txn %v is readonly does not need send to partition", info.txnId)
		return
	}

	log.Debugf("txn %v coordinator send commit to partition", info.txnId)
	partitionWriteKV := make(map[int][]*rpc.KeyValue)
	for _, kv := range info.commitRequestOp.request.WriteKeyValList {
		pId := c.server.config.GetPartitionIdByKey(kv.Key)
		if _, exist := partitionWriteKV[pId]; !exist {
			partitionWriteKV[pId] = make([]*rpc.KeyValue, 0)
		}
		partitionWriteKV[pId] = append(partitionWriteKV[pId], kv)
	}
	partitionReadVersion := make(map[int][]*rpc.KeyVersion)
	for _, kv := range info.commitRequestOp.request.ReadKeyVerList {
		pId := c.server.config.GetPartitionIdByKey(kv.Key)
		if _, exist := partitionReadVersion[pId]; !exist {
			partitionReadVersion[pId] = make([]*rpc.KeyVersion, 0)
		}
		partitionReadVersion[pId] = append(partitionReadVersion[pId], kv)
	}

	log.Debugf("txn %v coordinator send commit to partition %v",
		info.txnId, info.readRequest.Txn.ParticipatedPartitionIds)
	for _, pId := range info.readRequest.Txn.ParticipatedPartitionIds {
		request := &rpc.CommitRequest{
			TxnId:             info.txnId,
			WriteKeyValList:   partitionWriteKV[int(pId)],
			FromCoordinator:   true,
			ReadKeyVerList:    partitionReadVersion[int(pId)],
			IsReadAnyReplica:  false,
			IsFastPathSuccess: info.partitionPrepareResult[int(pId)].isFastPrepare,
		}

		log.Debugf("send to commit to pId %v, txn %v", pId, request.TxnId)

		if c.server.config.GetFastPath() && c.server.config.UseNetworkTimestamp() &&
			c.server.config.IsFastCommit() {
			// fast commit
			serverIdList := c.server.config.GetServerIdListByPartitionId(int(pId))
			maxDelay := c.predictOneWayLatency(serverIdList) * 1000000 // change to nanoseconds
			maxDelay += c.server.config.GetDelay().Nanoseconds()
			maxDelay += time.Now().UnixNano()

			fastRequest := &rpc.FastCommitRequest{
				CommitRequest: request,
				Timestamp:     maxDelay,
			}
			for _, serverId := range serverIdList {
				sender := NewFastCommitRequestSender(fastRequest, serverId, c.server)
				go sender.Send()
			}
		} else {
			serverId := c.server.config.GetLeaderIdByPartitionId(int(pId))
			sender := NewCommitRequestSender(request, serverId, c.server)
			go sender.Send()
		}
	}

	c.sendResultToCoordinator(info)
}

func (c *Coordinator) sendToParticipantsAndClient(info *TwoPCInfo) {
	if info.resultSent {
		log.Debugf("txn %v result is sent", info.txnId)
		return
	}
	info.resultSent = true
	log.Debugf("txn %v send result %v to client and partition", info.txnId, info.status.String())
	if info.status.IsAbort() {
		c.sendAbort(info)
	} else if info.status == COMMIT {
		c.sendCommit(info)
	} else {
		log.Fatalf("txn %v status %v should be commit or abort", info.txnId, info.status.String())
	}
}

func (c *Coordinator) reverserReorderPrepare(request *rpc.PrepareResultRequest) {
	txnId := request.TxnId
	twoPCInfo := c.txnStore[txnId]
	pId := int(request.PartitionId)
	log.Debugf("txn %v is reverse reorder prepare condition %v ",
		txnId, twoPCInfo.partitionPrepareResult[pId].prepareResult.Reorder)
	for _, txn := range twoPCInfo.partitionPrepareResult[pId].prepareResult.Reorder {
		if _, exist := twoPCInfo.partitionPrepareResult[pId].reorderAgreementReceived[txn]; !exist {
			log.Debugf("txn %v : txn %v cannot be reorder now wait", txnId, txn)
			return
		}
		log.Debugf("txn %v : txn %v agree to reorder", txnId, txn)
	}

	log.Debugf("txn %v all conditions %v satisfy",
		txnId, twoPCInfo.partitionPrepareResult[pId].prepareResult.Reorder)
	twoPCInfo.partitionPrepareResult[pId].status = PREPARED
	c.checkResult(twoPCInfo)
}

func (c *Coordinator) forwardPrepare(request *rpc.PrepareResultRequest) {
	txnId := request.TxnId
	twoPCInfo := c.txnStore[txnId]

	twoPCInfo.hasForward = true
	for _, txn := range request.Forward {
		twoPCInfo.dependTxns[txn] = true
	}

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) conditionalPrepare(request *rpc.PrepareResultRequest) {
	txnId := request.TxnId
	twoPCInfo := c.txnStore[txnId]

	twoPCInfo.hasCondition = true
	for c := range request.Conditions {
		twoPCInfo.conditionGraph.AddEdge(c, int(request.PartitionId))
	}

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) reorderPrepare(request *rpc.PrepareResultRequest) {
	txnId := request.TxnId
	twoPCInfo := c.txnStore[txnId]
	pId := int(request.PartitionId)

	if p, exist := twoPCInfo.partitionPrepareResult[pId]; exist {
		if p.status != REORDER_PREPARED && p.counter == request.Counter {
			log.Debugf("txn %v is already reverse the reorder, this is reordered prepare. ignore", txnId)
			c.sendRePrepare(txnId, "", pId, request.Counter)
			return
		}
	}

	c.checkResult(twoPCInfo)
}

func (c *Coordinator) sendRePrepare(txnId string, requestTxnId string, partitionId int, counter int32) {
	c.txnStore[txnId].rePrepare = true
	request := &rpc.RePrepareRequest{
		TxnId:        txnId,
		RequestTxnId: requestTxnId,
		Counter:      counter,
	}

	dstServerId := c.server.config.GetLeaderIdByPartitionId(partitionId)
	sender := NewRePrepareSender(request, dstServerId, c.server)
	go sender.Send()
}

func (c *Coordinator) sendReverseReorderAgreement(reverseReorderRequest *rpc.ReverseReorderRequest, canReverse bool) {
	request := &rpc.ReverseAgreementRequest{
		TxnId:          reverseReorderRequest.TxnId,
		ReorderedTxnId: reverseReorderRequest.ReorderedTxnId,
		AgreeReorder:   canReverse,
		PartitionId:    reverseReorderRequest.PartitionId,
		Counter:        reverseReorderRequest.Counter,
	}

	dstServerId := c.server.config.GetLeaderIdByPartitionId(int(reverseReorderRequest.CoordPartitionId))
	sender := NewReverseReorderAgreementSender(request, dstServerId, c.server)
	go sender.Send()
}

func (c *Coordinator) print() {
	if !c.server.IsLeader() {
		return
	}
	fName := fmt.Sprintf("s%v_coordinator.log", c.server.serverId)
	file, err := os.Create(fName)
	if err != nil {
		log.Fatalf("Fails to create log file %v error %v", fName, err)
		return
	}
	_, err = file.WriteString("#txnId commit/abort reorder-prepare condition-prepare reverse-reorder-prepare\n")
	if err != nil {
		log.Fatalf("cannot write to file %v error %v", fName, err)
	}
	for txnId, info := range c.txnStore {
		pResult := make(map[int]bool)
		fastPathUsed := make(map[int]bool)
		for pId, r := range info.partitionPrepareResult {
			pResult[pId] = r.isFastPrepare
			fastPathUsed[pId] = r.fastPrepareUsed
		}

		line := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v\n",
			txnId,
			info.status.String(),
			info.reorderPrepare,
			info.conditionPrepare,
			info.reversedReorderPrepare,
			info.reversedReorder,
			info.rePrepare,
			pResult,
			fastPathUsed,
		)
		_, err := file.WriteString(line)
		if err != nil {
			log.Fatalf("cannot write to file %v error %v", fName, err)
		}
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close file %v error %v", fName, err)
	}
}
