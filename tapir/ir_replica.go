package tapir

import (
	sync "sync"

	"github.com/op/go-logging"
)

type IrApp interface {
	ExecInconsistent(op interface{})
	ExecConsensus(op interface{}) interface{}
	ExecUnLog(op interface{}) interface{}

	// For testing
	Test()
}

type IrIncOp struct {
	op    interface{}
	state int
}

type IrConOp struct {
	op     interface{}
	result interface{}
	state  int
}

type IrReplica struct {
	app IrApp

	id      string
	isStart bool
	lock    sync.Mutex

	eventCh chan interface{} // input event channel

	// IR operation table
	incOpTable map[string]*IrIncOp
	conOpTable map[string]*IrConOp

	//// Helper data structure
	// Inconsistent operations whose Finalize arrives earlier than Proposal
	pendingIncOpTable map[string]bool        // ir inconsistent request id --> true
	pendingConOpTable map[string]interface{} // ir consensus request id --> execution result

	// Clock skew
	clockSkew int64
}

func NewIrReplica(
	id string,
	app IrApp,
	chSize int,
	clockSkew int64,
) *IrReplica {
	ir := &IrReplica{
		id:                id,
		app:               app,
		isStart:           false,
		eventCh:           make(chan interface{}, chSize),
		incOpTable:        make(map[string]*IrIncOp),
		conOpTable:        make(map[string]*IrConOp),
		pendingIncOpTable: make(map[string]bool),
		pendingConOpTable: make(map[string]interface{}),
		clockSkew:         clockSkew,
	}
	return ir
}

func (ir *IrReplica) GetApp() IrApp {
	return ir.app
}

func (ir *IrReplica) Start() {
	ir.lock.Lock()
	defer ir.lock.Unlock()

	if ir.isStart {
		return
	}
	ir.isStart = true

	go func() {
		for e := range ir.eventCh {
			ir.process(e)
		}
	}()
}

func (ir *IrReplica) schedule(e interface{}) {
	ir.eventCh <- e
}

func (ir *IrReplica) process(e interface{}) {
	switch e.(type) {
	case *UnlogOpEvent: // Unlog operatoins
		ir.processUnlogOp(e.(*UnlogOpEvent))
	case *IncOpProposal: // Proposed inconsistent operations
		ir.processIncOpProposal(e.(*IncOpProposal))
	case *IncOpFinalize: // Finalizes inconsisten operations
		ir.processIncOpFinalize(e.(*IncOpFinalize))
	case *ConOpProposal:
		ir.processConOpProposal(e.(*ConOpProposal))
	case *ConOpFinalize:
		ir.processConOpFinalize(e.(*ConOpFinalize))
	case *ProbeEvent:
		et := e.(*ProbeEvent)
		reply := ir.processProbeEvent(et)
		et.retCh <- reply
	case *TestEvent:
		ir.processTestEvent(e.(*TestEvent))
	default:
		logger.Fatalf("Unknown IR event %v", e)
	}
}

func (ir *IrReplica) processUnlogOp(e *UnlogOpEvent) {
	ret := ir.app.ExecUnLog(e.op)
	e.retCh <- ret
}

func (ir *IrReplica) processIncOpProposal(e *IncOpProposal) {
	//logger.Debugf("IR propose inconsistent request id = %s", e.id)
	if _, ok := ir.incOpTable[e.id]; ok {
		logger.Fatalf("IR inconsistent request id = %s already existed", e.id)
	}
	ir.incOpTable[e.id] = &IrIncOp{e.op, IR_OP_TENTATIVE}
	e.retCh <- true

	if _, ok := ir.pendingIncOpTable[e.id]; ok {
		// Finalize message arrives earlier
		delete(ir.pendingIncOpTable, e.id)
		ir.finalizeIncOp(e.id)
	}
}

func (ir *IrReplica) processIncOpFinalize(e *IncOpFinalize) {
	//logger.Debugf("IR finalize inconsistent request id = %s", e.id)
	if _, ok := ir.incOpTable[e.id]; ok {
		ir.finalizeIncOp(e.id)
	} else {
		// Proposal message has not arrived yet
		if _, ok := ir.pendingIncOpTable[e.id]; ok {
			logger.Fatalf("IR inconsistent request id = %s is already pending for finalization", e.id)
		}
		ir.pendingIncOpTable[e.id] = true
	}
}

func (ir *IrReplica) finalizeIncOp(id string) {
	irOp, ok := ir.incOpTable[id]
	if !ok {
		logger.Fatalf("IR inconsistent request id = %s does not exist, cannot execute", id)
	}
	ir.app.ExecInconsistent(irOp.op)
	irOp.state = IR_OP_FINALIZED

	delete(ir.incOpTable, id) // Saves memory for now.
	// TODO: Not delete until sync or not needed by recovery
	// TODO: Persist inconsistent operations
}

func (ir *IrReplica) processConOpProposal(e *ConOpProposal) {
	if _, ok := ir.conOpTable[e.id]; ok {
		logger.Fatalf("IR consensus request id = %s has been proposed", e.id)
	}

	ret := ir.app.ExecConsensus(e.op)

	if logger.IsEnabledFor(logging.DEBUG) {
		//opRet := ret.(*PrepareOpRet)
		//logger.Debugf("IR consensus reqId = %s result = %d %d", e.id, opRet.State, opRet.T)
	}

	ir.conOpTable[e.id] = &IrConOp{
		op:     e.op,
		result: ret,
		state:  IR_OP_TENTATIVE,
	}

	e.retCh <- ret

	if ret, ok := ir.pendingConOpTable[e.id]; ok {
		// The finalized message arrives earlier than the proposal
		delete(ir.pendingConOpTable, e.id)
		ir.finalizeConOp(e.id, ret)
	}
}

func (ir *IrReplica) processConOpFinalize(e *ConOpFinalize) {
	if _, ok := ir.conOpTable[e.id]; ok {
		ir.finalizeConOp(e.id, e.result)
	} else {
		// The finalized message arrives earlier than the proposal
		if _, ok := ir.pendingConOpTable[e.id]; ok {
			logger.Fatalf("IR consensus request id = %s is already pending for finalization", e.id)
		}
		ir.pendingConOpTable[e.id] = e.result
	}

	e.retCh <- true
}

func (ir *IrReplica) finalizeConOp(id string, result interface{}) {
	irConOp, ok := ir.conOpTable[id]
	if !ok {
		logger.Fatalf("IR consensus request id = %s does not exist, cannot finalize", id)
	}
	irConOp.state = IR_OP_FINALIZED
	irConOp.result = result

	delete(ir.conOpTable, id) // Saves memory for now.
	// TODO: Not delete until sync or not needed by recovery
	// TODO: Persist consensus operations
}

// For probing
func (ir *IrReplica) processProbeEvent(e *ProbeEvent) *ProbeReply {
	reply := &ProbeReply{
		T: ReadClockNano(GenClockSkew(ir.clockSkew)),
	}
	return reply
}

// For testing
func (ir *IrReplica) processTestEvent(e *TestEvent) {
	ir.doTest()
	e.retCh <- true
}

func (ir *IrReplica) doTest() {
	//logger.Infof("# of non-finalized inconsistent operations: %d", len(ir.incOpTable))
	//logger.Infof("# of pending inconsistent operations: %d", len(ir.pendingIncOpTable))
	//logger.Infof("# of non-finalized consensus operations: %d", len(ir.conOpTable))
	//logger.Infof("# of pending consensus operations: %d", len(ir.pendingConOpTable))

	// Print testing log for IR App
	ir.app.Test()
}
