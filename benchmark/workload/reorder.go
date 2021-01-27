package workload

import (
	"Carousel-GTS/utils"
	log "github.com/sirupsen/logrus"
	"strconv"
)

type ReorderWorkload struct {
	*AbstractWorkload

	txnQueue chan *Txn
	// number of partition
	partitionNum int

	localPartition int

	curIdx int64
}

func NewReorderWorkload(
	workload *AbstractWorkload,
	partitionNum int,
	localPartition int) *ReorderWorkload {
	rw := &ReorderWorkload{
		AbstractWorkload: workload,
		txnQueue:         make(chan *Txn, 1024),
		partitionNum:     partitionNum,
		localPartition:   localPartition,
	}

	log.Debugf("create reorder workload: localPartition: %v, number of partition %v",
		localPartition, partitionNum)

	return rw
}

func (rw *ReorderWorkload) GenTxn() *Txn {
	if len(rw.txnQueue) > 0 {
		return <-rw.txnQueue
	}

	if rw.curIdx == rw.KeyNum {
		rw.curIdx = 0
	}

	keyList := make([]int64, rw.partitionNum*2)
	for i := 0; i < rw.partitionNum*2; i++ {
		keyList[i] = rw.curIdx
		rw.curIdx++
	}

	txnList := make([][]string, rw.partitionNum*2-1)
	txnList[0] = make([]string, rw.partitionNum)
	for i := 0; i < rw.partitionNum; i++ {
		txnList[0][i] = utils.ConvertToString(rw.keySize, keyList[i])
	}
	log.Debugf("txn 0 keys: %v", txnList[0])
	txnList[1] = make([]string, rw.partitionNum*2)
	for i := 0; i < rw.partitionNum*2; i++ {
		txnList[1][i] = utils.ConvertToString(rw.keySize, keyList[i])
	}
	log.Debugf("txn 1 keys: %v", txnList[1])
	keyIdx := rw.partitionNum
	for i := 2; i < rw.partitionNum*2-1; i++ {
		txnList[i] = make([]string, 1)
		txnList[i][0] = utils.ConvertToString(rw.keySize, keyList[keyIdx])
		keyIdx++
		log.Debugf("txn %v keys: %v", i, txnList[i])
	}

	for i := 0; i < len(txnList); i++ {
		if i%rw.partitionNum != rw.localPartition {
			continue
		}
		log.Debugf("txn for local: %v", txnList[i])
		rw.txnCount++
		txnId := strconv.FormatInt(rw.txnCount, 10)
		txn := &Txn{
			TxnId:     txnId,
			ReadKeys:  txnList[i],
			WriteKeys: txnList[i],
			WriteData: make(map[string]string),
			Priority:  true,
		}
		rw.txnQueue <- txn
	}

	return <-rw.txnQueue
}

func (rw *ReorderWorkload) String() string {
	return "ReorderWorkload"
}
