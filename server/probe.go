package server

type ProbeOp struct {
	*ReadAndPrepareGTS
	//wait chan bool
}

func NewProbeOp() *ProbeOp {
	p := &ProbeOp{
		NewReadAndPrepareGTSProbe(),
	}
	return p
}

func (p *ProbeOp) Start(server *Server) {
	server.scheduler.AddOperation(p)
}

func (p *ProbeOp) Schedule(scheduler *Scheduler) {
	p.clientWait <- true
}
