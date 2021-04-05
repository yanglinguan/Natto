package workload

import (
	"Carousel-GTS/utils"
	"strconv"
)

// This workload always generates a transaction that has the same reads and writes
type OneTxnWorkload struct {
	*AbstractWorkload
	readNum  int
	writeNum int
}

func NewOneTxnWorkload(
	workload *AbstractWorkload,
	oneTxnReadNum int,
	oneTxnWriteNum int,
) *OneTxnWorkload {
	oneTxn := &OneTxnWorkload{
		AbstractWorkload: workload,
		readNum:          oneTxnReadNum,
		writeNum:         oneTxnWriteNum,
	}

	return oneTxn
}

// Generates a txn. This function is currently not thread-safe
func (oneTxn *OneTxnWorkload) GenTxn() Txn {
	oneTxn.txnCount++
	txnId := strconv.FormatInt(oneTxn.txnCount, 10)

	txn := &BaseTxn{
		txnId:     txnId,
		readKeys:  make([]string, 0),
		writeData: make(map[string]string),
	}

	// read keys
	for i := 0; i < oneTxn.readNum; i++ {
		txn.readKeys = append(txn.readKeys, utils.ConvertToString(oneTxn.keySize, int64(i)))
	}
	// write keys
	for i := 0; i < oneTxn.writeNum; i++ {
		//txn.WriteData[oneTxn.KeyList[i]] = oneTxn.KeyList[i]
		k := utils.ConvertToString(oneTxn.keySize, int64(i))
		txn.writeData[k] = k
	}

	return txn
}
