package client

import (
	"Carousel-GTS/rpc"
	"Carousel-GTS/utils"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"sync"
	"time"
)

type ReadAndPrepareSender struct {
	request        *rpc.ReadAndPrepareRequest
	txnId          string // this txnId may differ from request.Txn.TxnId when retry happens
	executionCount int64
	//execution   *ExecutionRecord
	timeout     time.Duration
	dstServerId int
	client      *Client
}

func NewReadAndPrepareSender(request *rpc.ReadAndPrepareRequest,
	executionCount int64, txnId string, dstServerId int, client *Client) *ReadAndPrepareSender {
	r := &ReadAndPrepareSender{
		request:        request,
		executionCount: executionCount,
		timeout:        0,
		txnId:          txnId,
		dstServerId:    dstServerId,
		client:         client,
	}

	return r
}

func (s *ReadAndPrepareSender) Send() {
	conn := s.client.connections[s.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)

	logrus.Debugf("SEND ReadAndPrepare %v to %v (%v) delay %v est %v pId %v",
		s.request.Txn.TxnId, conn.GetDstAddr(), s.dstServerId,
		s.request.Timestamp,
		s.request.Txn.EstimateArrivalTimes,
		s.request.Txn.ParticipatedPartitionIds)

	reply, err := client.ReadAndPrepare(context.Background(), s.request)
	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			logrus.Debugf("resend ReadAndPrepare %v to %v", s.request.Txn.TxnId, dstServerId)
			s.dstServerId = dstServerId
			s.Send()
		} else {
			logrus.Fatalf("cannot send txn %v readAndPrepare to server %v: %v", s.request.Txn.TxnId, conn.GetDstAddr(), err)
		}
	} else {
		logrus.Debugf("RECEIVE ReadResult %v from %v status %v kv %v",
			s.request.Txn.TxnId, conn.GetDstAddr(), reply.Status, reply.KeyValVerList)

		op := NewReadAndPrepareReplyOp(s.txnId, s.executionCount, reply)
		s.client.AddOperation(op)
		//s.execution.readAndPrepareReply <- reply
	}
}

type ReadOnlySender struct {
	request        *rpc.ReadAndPrepareRequest
	executionCount int64
	txnId          string // this txnId may differ from request.Txn.TxnId when retry happens
	timeout        time.Duration
	dstServerId    int
	client         *Client
}

func NewReadOnlySender(request *rpc.ReadAndPrepareRequest,
	executionCount int64, txnId string, dstServerId int, client *Client) *ReadOnlySender {
	s := &ReadOnlySender{
		request:        request,
		executionCount: executionCount,
		timeout:        0,
		txnId:          txnId,
		dstServerId:    dstServerId,
		client:         client,
	}

	return s
}

func (s *ReadOnlySender) Send() {
	conn := s.client.connections[s.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	c := rpc.NewCarouselClient(clientConn)
	logrus.Infof("SEND ReadOnly %v to %v (%v)", s.request.Txn.TxnId, conn.GetDstAddr(), s.dstServerId)

	reply, err := c.ReadOnly(context.Background(), s.request)
	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			logrus.Debugf("resend ReadOnly %v to %v", s.request.Txn.TxnId, dstServerId)
			s.dstServerId = dstServerId
			s.Send()
		} else {
			logrus.Fatalf("cannot send txn %v ReadOnly to server %v: %v", s.request.Txn.TxnId, conn.GetDstAddr(), err)
		}
	} else {
		logrus.Infof("RECEIVE ReadOnly %v from %v status %v", s.request.Txn.TxnId, conn.GetDstAddr(), reply.Status)
		op := NewReadOnlyReplyOp(s.txnId, s.executionCount, reply)
		s.client.AddOperation(op)
		//s.execution.readAndPrepareReply <- reply
	}
}

type CommitRequestSender struct {
	request *rpc.CommitRequest
	//txn         *Transaction
	txnId       string
	timeout     time.Duration
	dstServerId int
	client      *Client
}

func NewCommitRequestSender(request *rpc.CommitRequest,
	txnId string, dstServerId int, client *Client) *CommitRequestSender {
	s := &CommitRequestSender{
		request:     request,
		txnId:       txnId,
		timeout:     0,
		dstServerId: dstServerId,
		client:      client,
	}
	return s
}

func (c *CommitRequestSender) Send() {
	conn := c.client.connections[c.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	cli := rpc.NewCarouselClient(clientConn)
	logrus.Infof("SEND Commit %v to %v", c.request.TxnId, conn.GetDstAddr())
	reply, err := cli.Commit(context.Background(), c.request)
	if err == nil {
		logrus.Infof("RECEIVE CommitResult %v from %v", c.request.TxnId, conn.GetDstAddr())
		op := NewCommitReplyOp(c.txnId, reply)
		c.client.AddOperation(op)
		//c.txn.commitReply <- reply
	} else {
		if dstServerId, handled := utils.HandleError(err); handled {
			logrus.Debugf("resend ReadAndPrepare %v to %v", c.request.TxnId, dstServerId)
			c.dstServerId = dstServerId
			c.Send()
		} else {
			logrus.Fatalf("cannot send txn %v commit request to server %v: %v", c.request.TxnId, conn.GetDstAddr(), err)
		}
	}
}

type PrintStatusRequestSender struct {
	request     *rpc.PrintStatusRequest
	dstServerId int
	client      *Client
}

func NewPrintStatusRequestSender(request *rpc.PrintStatusRequest, dstServerId int, client *Client) *PrintStatusRequestSender {
	p := &PrintStatusRequestSender{
		request:     request,
		dstServerId: dstServerId,
		client:      client,
	}
	return p
}

func (p *PrintStatusRequestSender) Send(wg *sync.WaitGroup) {
	conn := p.client.connections[p.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	logrus.Infof("SEND PrintStatus to %v %v", conn.GetDstAddr(), p.request.CommittedTxn)
	_, err := client.PrintStatus(context.Background(), p.request)
	if err != nil {
		logrus.Fatalf("cannot sent print status request to server %v: %v", conn.GetDstAddr(), err)
	}
	logrus.Infof("RECEIVE PrintStatus reply from %v", conn.GetDstAddr())
	wg.Done()
}

type HeartBeatSender struct {
	dstServerId int
	client      *Client
	leaderId    int
}

func NewHeartBeatSender(dstServerId int, client *Client) *HeartBeatSender {
	h := &HeartBeatSender{
		dstServerId: dstServerId,
		client:      client,
	}
	return h
}

func (h *HeartBeatSender) Send() int {
	conn := h.client.connections[h.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	logrus.Infof("SEND HeartBeat to %v ", conn.GetDstAddr())
	reply, err := client.HeartBeat(context.Background(), &rpc.Empty{})
	if err != nil {
		logrus.Fatalf("cannot sent heart beat request to server %v: %v", conn.GetDstAddr(), err)
		return -1
	}
	return int(reply.LeaderId)
}

type ProbeSender struct {
	dstServerId int
	client      *Client
}

func NewProbeSender(dstServerId int, client *Client) *ProbeSender {
	s := &ProbeSender{
		dstServerId: dstServerId,
		client:      client,
	}
	return s
}

func (p *ProbeSender) Send() int64 {
	conn := p.client.connections[p.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	reply, err := client.Probe(context.Background(), &rpc.ProbeReq{
		FromCoordinator: false,
	})
	if err != nil {
		logrus.Fatalf("cannot sent probe request to server %v: %v", conn.GetDstAddr(), err)
		return -1
	}
	return reply.QueuingDelay
}

type ProbeTimeSender struct {
	dstServerId int
	client      *Client
}

func NewProbeTimeSender(dstServerId int, client *Client) *ProbeTimeSender {
	s := &ProbeTimeSender{
		dstServerId: dstServerId,
		client:      client,
	}
	return s
}

func (p *ProbeTimeSender) Send() int64 {
	conn := p.client.connections[p.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	reply, err := client.ProbeTime(context.Background(), &rpc.ProbeReq{
		FromCoordinator: false,
	})
	if err != nil {
		logrus.Fatalf("cannot sent probeTime request to server %v: %v", conn.GetDstAddr(), err)
		return -1
	}
	return reply.ProcessTime
}

type StartProbeSender struct {
	dstServerId int
	client      *Client
}

func NewStartProbeSender(dstServerId int, client *Client) *StartProbeSender {
	s := &StartProbeSender{
		dstServerId: dstServerId,
		client:      client,
	}
	return s
}

func (p *StartProbeSender) Send() {
	conn := p.client.connections[p.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.StartProbe(context.Background(), &rpc.StartProbeReq{})
	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			logrus.Debugf("resend startProbe to %v", dstServerId)
			p.dstServerId = dstServerId
			p.Send()
		} else {
			logrus.Fatalf("cannot send startProbe server %v: %v", conn.GetDstAddr(), err)
		}
	}
}

type WriteDataSender struct {
	request     *rpc.WriteDataRequest
	dstServerId int
	client      *Client
}

func NewWriteDataSender(request *rpc.WriteDataRequest, dstServerId int, client *Client) *WriteDataSender {
	r := &WriteDataSender{
		request:     request,
		dstServerId: dstServerId,
		client:      client,
	}
	return r
}

func (w *WriteDataSender) Send() {
	conn := w.client.connections[w.dstServerId]
	clientConn := conn.GetConn()
	if conn.GetPoolSize() > 0 {
		defer conn.Close(clientConn)
	}

	client := rpc.NewCarouselClient(clientConn)
	_, err := client.WriteData(context.Background(), w.request)
	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			logrus.Debugf("resend startProbe to %v", dstServerId)
			w.dstServerId = dstServerId
			w.Send()
		} else {
			logrus.Fatalf("cannot send startProbe server %v: %v", conn.GetDstAddr(), err)
		}
	}
}
