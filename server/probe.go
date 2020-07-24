package server

type ProbeOp struct {
	*ReadAndPrepareBase
}

func NewProbeOp() *ProbeOp {
	p := &ProbeOp{
		NewReadAndPrepareBase(nil),
	}
	return p
}

func (p *ProbeOp) Start(server *Server) {
	server.scheduler.AddOperation(p)
}

func (p *ProbeOp) Execute(storage *Storage) {
	p.clientWait <- true
}
