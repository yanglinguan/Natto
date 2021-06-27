package spanner

type txnStore struct {
	transactions map[string]*transaction
}

func newTxnStore() *txnStore {
	t := &txnStore{transactions: make(map[string]*transaction)}
	return t
}

func (ts *txnStore) addTxn(txn *transaction) {
	ts.transactions[txn.txnId] = txn
}

func (ts *txnStore) getTxnById(txnId string) *transaction {
	return ts.transactions[txnId]
}

func (ts *txnStore) createTxn(txnId string, timestamp int64, cId int64) *transaction {
	if _, exist := ts.transactions[txnId]; !exist {
		ts.transactions[txnId] = NewTransaction(txnId, timestamp, cId)
	}
	return ts.transactions[txnId]
}
