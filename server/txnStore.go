package server

type TxnStore struct {
	txnStore map[string]*TxnInfo
}
