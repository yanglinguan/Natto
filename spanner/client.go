package spanner

import (
	"Carousel-GTS/configuration"
	"context"
	"github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

// client lib
type Client struct {
	config      configuration.Configuration
	connections []*connection
	clientId    int64
	dcId        int
}

func NewClient(clientId int, config configuration.Configuration) *Client {
	c := &Client{
		config:      config,
		connections: make([]*connection, config.GetServerNum()),
		clientId:    int64(clientId),
		dcId:        config.GetDataCenterIdByClientId(clientId),
	}
	// create connection to each server
	for sId, addr := range config.GetServerAddress() {
		logrus.Debugf("add connection server %v, addr %v", sId, addr)
		c.connections[sId] = newConnect(addr)
	}
	return c
}

func (c *Client) Begin(txnId string) *transaction {
	txn := NewTransaction(txnId, time.Now().UnixNano(), c.clientId)
	return txn
}

// send read request to partition leaders
func (c *Client) sendRead(txn *transaction, readKeys []string, pId int) {
	defer txn.wg.Done()
	leaderId := c.config.GetLeaderIdByPartitionId(pId)
	conn := c.connections[leaderId]

	client := NewSpannerClient(conn.GetConn())
	readRequest := &ReadRequest{
		Id:   txn.txnId,
		Keys: readKeys,
		Ts:   txn.timestamp,
		CId:  txn.clientId,
	}
	result, err := client.Read(context.Background(), readRequest)
	if err != nil {
		logrus.Debugf("txn %v cannot send read request to server %v pid %v", txn.txnId, leaderId, pId)
	} else {
		txn.lock.Lock()
		if result.Abort {
			txn.Status = ABORTED
		} else {
			for i, kv := range result.Vals {
				key := readKeys[i]
				txn.readResult[key] = kv
			}
		}
		txn.lock.Unlock()
	}
}

// returns false if txn aborted; otherwise returns true and read result
func (c *Client) Read(txn *transaction, rSet map[int][]string) (bool, map[string]string) {
	for pId, readKeys := range rSet {
		txn.participantPartition[pId] = true
		txn.wg.Add(1)
		go c.sendRead(txn, readKeys, pId)
	}
	// wait until all read requests get results
	txn.wg.Wait()
	// if txn abort returns nil; otherwise returns read result
	if txn.Status == ABORTED {
		return false, nil
	}
	result := make(map[string]string)
	for k, kv := range txn.readResult {
		result[k] = kv.Val
	}
	return true, result
}

// send commit request to partition leaders
func (c *Client) sendCommit(txn *transaction, readKeyVer []*KeyVer, pp []int32, wKeys map[string]string, pId int) bool {
	i := 0
	// create write data
	writeKeyVal := make([]*KeyVal, len(wKeys))
	i = 0
	for key, val := range wKeys {
		writeKeyVal[i] = &KeyVal{
			Key: key,
			Val: val,
		}
		i++
	}

	commitRequest := &CommitRequest{
		Id:       txn.txnId,
		Ts:       txn.timestamp,
		CId:      txn.clientId,
		RKV:      readKeyVer,
		WKV:      writeKeyVal,
		Pp:       pp,
		CoordPId: int64(txn.coordPId),
	}

	leaderId := c.config.GetLeaderIdByPartitionId(pId)
	conn := c.connections[leaderId]

	client := NewSpannerClient(conn.GetConn())
	result, err := client.Commit(context.Background(), commitRequest)
	if err != nil {
		logrus.Fatalf("txn %v cannot send commit request to server %v pId %v", txn.txnId, leaderId, pId)
		return false
	} else {
		return result.Commit
	}
}

func getWriteSet(wSet map[int]map[string]string, pId int) map[string]string {
	if _, exist := wSet[pId]; exist {
		return wSet[pId]
	}
	return make(map[string]string)
}

func (c *Client) findCoordinatorPartitionId(participants map[int]bool) int {
	// find leaders in the client's datacenter
	leaderIdList := c.config.GetLeaderIdListByDataCenterId(c.dcId)
	// use the participant partition if possible
	for _, sId := range leaderIdList {
		pId := c.config.GetPartitionIdByServerId(sId)
		if _, exist := participants[pId]; exist {
			return pId
		}
	}
	// if there is no participant partition in the client's datacenter
	// randomly select a leader in the client's datacenter
	return c.config.GetPartitionIdByServerId(leaderIdList[rand.Intn(len(leaderIdList))])
}

func (c *Client) Commit(txn *transaction, wSet map[int]map[string]string) bool {
	for pId := range wSet {
		txn.participantPartition[pId] = true
	}
	pp := make([]int32, len(txn.participantPartition))
	i := 0
	for pId := range txn.participantPartition {
		pp[i] = int32(pId)
		i++
	}

	i = 0
	readKeyVer := make([]*KeyVer, len(txn.readResult))
	for key, kv := range txn.readResult {
		readKeyVer[i] = &KeyVer{
			Key: key,
			Ver: kv.Ver,
		}
		i++
	}

	// for the single partition transaction, wait until the partition leader
	// returns commit result
	if len(txn.participantPartition) == 1 {
		for pId := range txn.participantPartition {
			return c.sendCommit(txn, readKeyVer, pp, getWriteSet(wSet, pId), pId)
		}
	}

	// assign coordinator partition
	coordPId := c.findCoordinatorPartitionId(txn.participantPartition)
	txn.coordPId = coordPId
	// for the multi-partition transaction,
	// send to participant partition leader without waiting the result
	for pId := range txn.participantPartition {
		if pId == txn.coordPId {
			continue
		}
		go c.sendCommit(txn, readKeyVer, pp, getWriteSet(wSet, pId), pId)
	}
	// wait the coordinator the commit result
	return c.sendCommit(txn, readKeyVer, pp, getWriteSet(wSet, txn.coordPId), txn.coordPId)
}

func (c *Client) sendAbort(txn *transaction, pId int) {
	defer txn.wg.Done()
	leaderId := c.config.GetLeaderIdByPartitionId(pId)
	abortRequest := &AbortRequest{
		Id:  txn.txnId,
		Ts:  txn.timestamp,
		CId: txn.clientId,
	}
	conn := c.connections[leaderId]
	client := NewSpannerClient(conn.GetConn())
	_, err := client.Abort(context.Background(), abortRequest)
	if err != nil {
		logrus.Fatalf("txn %v cannot send abort to server %v partition %v", txn.txnId, leaderId, pId)
	}
}

func (c *Client) Abort(txn *transaction) {
	for pId := range txn.participantPartition {
		txn.wg.Add(1)
		go c.sendAbort(txn, pId)
	}
	txn.wg.Wait()
}
