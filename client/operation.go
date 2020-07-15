package client

type Operation interface {
	Execute(client *Client)
}

type ReadOp interface {
	Operation

	GetReadResult() map[string]string
	GetReadKeyList() []string
	GetWriteKeyList() []string
	GetPriority() bool
	SetKeyValue(key, value string)
	SetAbort(abort bool)
	ClearReadKeyList()  // set read key List to nil
	ClearWriteKeyList() //set write key list to nil

	GetTxnId() string
	IsAbort() bool

	Block()
	Unblock()
}

//type CommitOp interface {
//	Operation
//
//	GetResult() (bool, bool, time.Duration, time.Duration)
//	SetResult(result bool, isRetry bool, retryWaitTime time.Duration, expWait time.Duration)
//
//	Block()
//	Unblock()
//}
