package spanner

import (
	"Carousel-GTS/configuration"
	"context"
	"github.com/sirupsen/logrus"
	"time"
)

type Client struct {
	config      configuration.Configuration
	connections []*connection
	clientId    int64
}

func NewClient(clientId int, config configuration.Configuration) *Client {
	c := &Client{
		config:      config,
		connections: make([]*connection, config.GetServerNum()),
		clientId:    int64(clientId),
	}

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

func (c *Client) Read(txn *transaction, rSet map[int][]string) (bool, map[string]string) {
	for pId, readKeys := range rSet {
		txn.participantPartition[pId] = true
		txn.wg.Add(1)
		go c.sendRead(txn, readKeys, pId)
	}
	txn.wg.Wait()
	if txn.Status == ABORTED {
		return false, nil
	}
	result := make(map[string]string)
	for k, kv := range txn.readResult {
		result[k] = kv.Val
	}
	return true, result
}

func (c *Client) sendCommit(txn *transaction, pp []int32, wKeys map[string]string, pId int) bool {
	readKeyVer := make([]*KeyVer, len(txn.readResult))
	i := 0
	for key, kv := range txn.readResult {
		readKeyVer[i] = &KeyVer{
			Key: key,
			Ver: kv.Ver,
		}
		i++
	}

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

func (c *Client) Commit(txn *transaction, wSet map[int]map[string]string) bool {
	for pId := range wSet {
		txn.participantPartition[pId] = true
	}
	pp := make([]int32, len(txn.participantPartition))
	i := 0
	for pId := range txn.participantPartition {
		pp[i] = int32(pId)
	}

	if len(txn.participantPartition) == 1 {
		for pId := range txn.participantPartition {
			return c.sendCommit(txn, pp, getWriteSet(wSet, pId), pId)
		}
	}
	for pId := range txn.participantPartition {
		if pId == txn.coordPId {
			continue
		}
		go c.sendCommit(txn, pp, getWriteSet(wSet, pId), pId)
	}

	return c.sendCommit(txn, pp, getWriteSet(wSet, txn.coordPId), txn.coordPId)
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
