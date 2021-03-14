package workload

import (
	"math/rand"
	"strconv"
)

type RandSizeYcsbt struct {
	*AbstractWorkload
	readNumPerTxn  int
	writeNumPerTxn int
}

func NewRandSizeYcsbWorkload(
	workload *AbstractWorkload,
	YCSBTReadNumPerTxn int,
	YCSBTWriteNumPerTxn int,
) *RandSizeYcsbt {
	ycsbt := &RandSizeYcsbt{
		AbstractWorkload: workload,
		readNumPerTxn:    YCSBTReadNumPerTxn,
		writeNumPerTxn:   YCSBTWriteNumPerTxn,
	}

	return ycsbt
}

// Generates a txn. This function is currently not thread-safe
func (ycsbt *RandSizeYcsbt) GenTxn() *Txn {
	ycsbt.txnCount++
	txnId := strconv.FormatInt(ycsbt.txnCount, 10)
	size := rand.Intn(ycsbt.readNumPerTxn) + 1
	return ycsbt.buildTxn(txnId, size, size)
}

func (ycsbt *RandSizeYcsbt) String() string {
	return "RandSizeYcsbtWorkload"
}
