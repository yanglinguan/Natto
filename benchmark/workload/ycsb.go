package workload

import (
	"strconv"
)

// Acknowledgement: this implementation derives from the YCSB+T workload.
// Currently, it only supports a fixed number of read-modify-write operations per transaction.
// TODO implements the full YCSB+T workload, like varying the probability of reads/writes.
type YCSBTWorkload struct {
	*AbstractWorkload
	readNumPerTxn  int
	writeNumPerTxn int
}

func NewYCSBTWorkload(
	workload *AbstractWorkload,
	YCSBTReadNumPerTxn int,
	YCSBTWriteNumPerTxn int,
) *YCSBTWorkload {
	ycsbt := &YCSBTWorkload{
		AbstractWorkload: workload,
		readNumPerTxn:    YCSBTReadNumPerTxn,
		writeNumPerTxn:   YCSBTWriteNumPerTxn,
	}

	return ycsbt
}

// Generates a txn. This function is currently not thread-safe
func (ycsbt *YCSBTWorkload) GenTxn() Txn {
	ycsbt.txnCount++
	txnId := strconv.FormatInt(ycsbt.txnCount, 10)
	return ycsbt.buildTxn(txnId, ycsbt.readNumPerTxn, ycsbt.readNumPerTxn)
}

func (ycsbt *YCSBTWorkload) String() string {
	return "YcsbtWorkload"
}
