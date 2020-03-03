package server

import (
	"container/list"
	"fmt"
	"log"
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
	txnId := make([]string, committed)
	for id, info := range txnStore {
		if info.status == COMMIT {
			txnId[info.commitOrder] = id
		}
	}

	file, err := os.Create(serverId + "_commitOrder.log")
	if err != nil || file == nil {
		log.Fatal("Fails to create log file: statistic.log")
		return
	}

	for _, id := range txnId {
		_, err = file.WriteString(id + "\n")
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
