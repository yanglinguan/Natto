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
	request     *rpc.ReadAndPrepareRequest
	execution   *ExecutionRecord
	timeout     time.Duration
	dstServerId int
	client      *Client
}

func NewReadAndPrepareSender(request *rpc.ReadAndPrepareRequest,
	txn *ExecutionRecord, dstServerId int, client *Client) *ReadAndPrepareSender {
	r := &ReadAndPrepareSender{
		request:     request,
		execution:   txn,
		timeout:     0,
		dstServerId: dstServerId,
		client:      client,
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
	logrus.Infof("SEND ReadAndPrepare %v to %v", s.request.Txn.TxnId, conn.GetDstAddr())

	reply, err := client.ReadAndPrepare(context.Background(), s.request)
	if err != nil {
		if dstServerId, handled := utils.HandleError(err); handled {
			s.dstServerId = dstServerId
			s.Send()
		} else {
			logrus.Fatalf("cannot send txn %v readAndPrepare to server %v: %v", s.request.Txn.TxnId, conn.GetDstAddr(), err)
		}
	} else {
		logrus.Infof("RECEIVE ReadResult %v from %v isAbort %v", s.request.Txn.TxnId, conn.GetDstAddr(), reply.IsAbort)
		s.execution.readAndPrepareReply <- reply
	}
}

type CommitRequestSender struct {
	request     *rpc.CommitRequest
	txn         *Transaction
	timeout     time.Duration
	dstServerId int
	client      *Client
}

func NewCommitRequestSender(request *rpc.CommitRequest,
	txn *Transaction, dstServerId int, client *Client) *CommitRequestSender {
	s := &CommitRequestSender{
		request:     request,
		txn:         txn,
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

	client := rpc.NewCarouselClient(clientConn)
	logrus.Infof("SEND Commit %v to %v", c.request.TxnId, conn.GetDstAddr())
	reply, err := client.Commit(context.Background(), c.request)
	if err == nil {
		logrus.Infof("RECEIVE CommitResult %v from %v", c.request.TxnId, conn.GetDstAddr())
		c.txn.commitReply <- reply
	} else {
		if dstServerId, handled := utils.HandleError(err); handled {
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
	logrus.Infof("SEND PrintStatus to %v ", conn.GetDstAddr())
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
