package server

type PrintStatusRequest struct {
	committedTxn int
	wait         chan bool
}

func NewPrintStatusRequestOp(committedTxn int) *PrintStatusRequest {
	p := &PrintStatusRequest{
		committedTxn: committedTxn,
		wait:         make(chan bool, 1),
	}

	return p
}

func (o *PrintStatusRequest) Execute(storage *Storage) {
	storage.waitPrintStatusRequest = o
	storage.totalCommit = o.committedTxn
	storage.print()
}

func (o *PrintStatusRequest) BlockClient() {
	<-o.wait
}

func (o *PrintStatusRequest) UnblockClient() {
	o.wait <- true
}
