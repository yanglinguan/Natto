package server

import (
	"container/list"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
)

type KeyType int

const (
	READ = iota
	WRITE
)

type TxnStatus int

const (
	INIT = iota
	PREPARED
	COMMIT
	ABORT
)

type TxnInfo struct {
	readAndPrepareRequestOp *ReadAndPrepareOp
	status                  TxnStatus
	receiveFromCoordinator  bool
	commitOrder             int
	waitingTxn              int
}

type ValueVersion struct {
	Value            string
	Version          uint64
	WaitingOp        *list.List
	WaitingItem      map[string]*list.Element
	PreparedTxnRead  map[string]bool
	PreparedTxnWrite map[string]bool
}

type Storage interface {
	Prepare(op *ReadAndPrepareOp)
	Commit(op *CommitRequestOp)
	Abort(op *AbortRequestOp)
	LoadKeys(keys []string)
	PrintStatus(op *PrintStatusRequestOp)
}

func printCommitOrder(txnStore map[string]*TxnInfo, committed int, serverId string) {
	txnId := make([]*TxnInfo, committed)
	for _, info := range txnStore {
		if info.status == COMMIT {
			txnId[info.commitOrder] = info
		}
	}

	file, err := os.Create(serverId + "_commitOrder.log")
	if err != nil || file == nil {
		log.Fatal("Fails to create log file: statistic.log")
		return
	}

	for _, info := range txnId {
		s := fmt.Sprintf("%v %v\n", info.readAndPrepareRequestOp.request.Txn.TxnId, info.waitingTxn)
		_, err = file.WriteString(s)
		if err != nil {
			log.Fatalf("Cannot write to file %v", err)
		}
	}

	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close file %v", err)
	}
}

func printModifiedData(kvStore map[string]*ValueVersion, serverId string) {
	file, err := os.Create(serverId + "_db.log")
	if err != nil || file == nil {
		log.Fatal("Fails to create log file: statistic.log")
		return
	}

	_, err = file.WriteString("#key, value, version\n")
	if err != nil {
		log.Fatalf("Cannot write to file, %v", err)
		return
	}

	for key, kv := range kvStore {
		if kv.Version == 0 {
			continue
		}

		var k int
		_, err := fmt.Sscan(key, &k)
		if err != nil {
			log.Fatalf("key %v is invalid", key)
		}

		var v int
		_, err = fmt.Sscan(kv.Value, &v)
		if err != nil {
			log.Fatalf("value %v is invalid", kv.Value)
		}

		s := fmt.Sprintf("%v,%v,%v\n",
			k,
			v,
			kv.Version)
		_, err = file.WriteString(s)
		if err != nil {
			log.Fatalf("fail to write %v", err)
		}
	}

	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close file %v", err)
	}
}
