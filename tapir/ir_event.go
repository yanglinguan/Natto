package tapir

// Events for unlog operations
type UnlogOpEvent struct {
	op    interface{}      // Operation
	retCh chan interface{} // Operation execution result channel
}

// Events for inconsistent operations
type IncOpProposal struct {
	id    string      // IR request id
	op    interface{} // Operation
	retCh chan bool   // Operation completion ack channel
}

type IncOpFinalize struct {
	id string // IR request id
}

// Events for consensus operations
type ConOpProposal struct {
	id    string           // IR request id
	op    interface{}      // Operation
	retCh chan interface{} // Operation execution result channel
}

type ConOpFinalize struct {
	id     string      // IR request id
	result interface{} // Finalized operation execution result
	retCh  chan bool   // Completion ack channel
}

type ProbeEvent struct {
	retCh chan *ProbeReply
}

func NewProbeEvent() *ProbeEvent {
	return &ProbeEvent{
		retCh: make(chan *ProbeReply, 1),
	}
}

type TestEvent struct {
	retCh chan bool
}
