package tapir

import context "context"

// RPCs for unlog operations
func (s *Server) ProposeUnLogOp(ctx context.Context, msg *UnLogReqMsg) (*UnLogReplyMsg, error) {
	op := &ReadOp{}
	Decode(msg.Op, op) // Decoding in an RPC goroutine to increase parallelism
	e := &UnlogOpEvent{
		op:    op,
		retCh: make(chan interface{}, 1),
	}

	s.ir.schedule(e) // Schedule the unlog operation
	ret := <-e.retCh // Wait for execution result

	b := Encode(ret)
	reply := &UnLogReplyMsg{
		Ret: b,
	}

	return reply, nil
}

// RPCs for IR inconsistent operations
func (s *Server) ProposeIncOp(ctx context.Context, msg *IncReqMsg) (*ConfirmMsg, error) {
	op := &CommitOp{}
	Decode(msg.Req.Op, op) // Decoding in an RPC goroutine to increase parallelism
	e := &IncOpProposal{
		id:    msg.Req.Id,
		op:    op,
		retCh: make(chan bool, 1),
	}

	s.ir.schedule(e)
	<-e.retCh // Wait until the inconsistent operation is recorded

	return &ConfirmMsg{}, nil
}

func (s *Server) FinalizeIncOp(ctx context.Context, msg *FinalIncReqMsg) (*ConfirmMsg, error) {
	e := &IncOpFinalize{
		id: msg.Id,
	}

	s.ir.schedule(e)

	return &ConfirmMsg{}, nil
}

// RPCs for IR consensus operations
func (s *Server) ProposeConOp(ctx context.Context, msg *ConReqMsg) (*ConReplyMsg, error) {
	op := &PrepareOp{}
	Decode(msg.Req.Op, op) // Decoding in an RPC goroutine to increase parallelism
	e := &ConOpProposal{
		id:    msg.Req.Id,
		op:    op,
		retCh: make(chan interface{}, 1),
	}

	s.ir.schedule(e)
	ret := <-e.retCh

	b := EncodeToStr(ret)
	reply := &ConReplyMsg{Ret: b}
	return reply, nil
}

func (s *Server) FinalizeConOp(ctx context.Context, msg *FinalConReqMsg) (*ConfirmMsg, error) {
	conOpRet := &PrepareOpRet{}
	DecodeFromStr(msg.Ret, conOpRet) // Decoding in an RPC goroutine to increase parallelism
	e := &ConOpFinalize{
		id:     msg.Id,
		result: conOpRet,
		retCh:  make(chan bool, 1),
	}

	s.ir.schedule(e)
	<-e.retCh

	return &ConfirmMsg{}, nil
}

func (s *Server) Probe(ctx context.Context, req *ProbeReq) (*ProbeReply, error) {
	e := NewProbeEvent()
	s.ir.schedule(e)
	reply := <-e.retCh
	return reply, nil
}

func (s *Server) Test(ctx context.Context, msg *TestMsg) (*TestMsg, error) {
	e := &TestEvent{
		retCh: make(chan bool, 1),
	}
	s.ir.schedule(e)
	<-e.retCh
	return &TestMsg{}, nil
}
