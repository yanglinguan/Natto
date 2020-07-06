package server

type ProbeOp struct {
	wait chan bool
}

func NewProbeOp() *ProbeOp {
	p := &ProbeOp{wait: make(chan bool)}
	return p
}

func (p *ProbeOp) Schedule(scheduler *Scheduler) {
	p.wait <- true
}

func (p *ProbeOp) BlockClient() {
	<-p.wait
}
