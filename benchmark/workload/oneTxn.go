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
func (oneTxn *OneTxnWorkload) GenTxn() *Txn {
	oneTxn.txnCount++
	txnId := strconv.FormatInt(oneTxn.txnCount, 10)

	txn := &Txn{
		TxnId:     txnId,
		ReadKeys:  make([]string, 0),
		WriteData: make(map[string]string),
	}

	// read keys
	for i := 0; i < oneTxn.readNum; i++ {
		txn.ReadKeys = append(txn.ReadKeys, utils.ConvertToString(oneTxn.keySize, int64(i)))
	}
	// write keys
	for i := 0; i < oneTxn.writeNum; i++ {
		//txn.WriteData[oneTxn.KeyList[i]] = oneTxn.KeyList[i]
		k := utils.ConvertToString(oneTxn.keySize, int64(i))
		txn.WriteData[k] = k
	}

	return txn
}
