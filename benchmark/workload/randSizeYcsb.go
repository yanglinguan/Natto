package workload

import (
	"math/rand"
	"strconv"
)

type RandSizeYcsbt struct {
	*AbstractWorkload
	readNumPerTxn  int
	writeNumPerTxn int
	single         int
}

func NewRandSizeYcsbWorkload(
	workload *AbstractWorkload,
	YCSBTReadNumPerTxn int,
	YCSBTWriteNumPerTxn int,
	single int,
) *RandSizeYcsbt {
	ycsbt := &RandSizeYcsbt{
		AbstractWorkload: workload,
		readNumPerTxn:    YCSBTReadNumPerTxn,
		writeNumPerTxn:   YCSBTWriteNumPerTxn,
		single:           single,
	}

	return ycsbt
}

// Generates a txn. This function is currently not thread-safe
func (ycsbt *RandSizeYcsbt) GenTxn() *Txn {
	ycsbt.txnCount++
	txnId := strconv.FormatInt(ycsbt.txnCount, 10)
	r := rand.Intn(100)
	size := 1
	if r >= ycsbt.single {
		size = rand.Intn(ycsbt.readNumPerTxn-2+1) + 2
	}

	return ycsbt.buildTxn(txnId, size, size)
}

func (ycsbt *RandSizeYcsbt) String() string {
	return "RandSizeYcsbtWorkload"
}
