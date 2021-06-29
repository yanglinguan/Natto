package spanner

import (
	"context"
	"github.com/sirupsen/logrus"
)

func (s *Server) Read(ctx context.Context, request *ReadRequest) (*ReadReply, error) {
	logrus.Debugf("txn %v receive read request from client %v", request.Id, request.CId)
	op := s.opCreator.createReadOp(request)
	s.addOp(op)

	op.wait()
	abort, result := op.getReadResult()
	reply := &ReadReply{
		Abort: abort,
		Vals:  result,
	}

	return reply, nil
}

func (s *Server) Commit(ctx context.Context, request *CommitRequest) (*CommitReply, error) {
	logrus.Debugf("receive txn %v commit request from client %v", request.Id, request.CId)
	// single partition txn
	if len(request.Pp) == 1 {
		op := s.opCreator.createCommitOp(request)
		s.addOp(op)
		op.wait()
		result := &CommitReply{
			Commit: op.getCommitResult(),
		}
		return result, nil
	}

	for _, pId := range request.Pp {
		if int(pId) == s.pId {
			op := s.opCreator.createCommitOp(request)
			s.addOp(op)
			break
		}
	}

	// wait for the coordinator commit result
	if s.pId == int(request.CoordPId) {
		op := &commitCoord{
			commitRequest: request,
			server:        s,
			result:        false,
			waitChan:      make(chan bool),
		}
		s.coordinator.addOp(op)
		op.wait()
		result := &CommitReply{
			Commit: op.getCommitResult(),
		}
		return result, nil
	}

	return &CommitReply{
		Commit: false,
	}, nil

}

func (s *Server) Prepare(ctx context.Context, request *PrepareRequest) (*Empty, error) {
	logrus.Debugf("receive txn %v coord receives prepare from pId %v, status %v",
		request.Id, request.PId, request.Prepared)
	op := &prepare{prepareRequest: request}
	s.coordinator.addOp(op)
	return &Empty{}, nil
}

func (s *Server) CommitDecision(ctx context.Context, request *CommitResult) (*Empty, error) {
	logrus.Debugf("receive txn %v commit decision from coord status %v",
		request.Id, request.Commit)
	op := &commitDecision{commitResult: request}
	s.addOp(op)
	return &Empty{}, nil
}

func (s *Server) Abort(ctx context.Context, request *AbortRequest) (*Empty, error) {
	logrus.Debugf("receive txn %v abort from client %v", request.Id, request.CId)
	op := &abortClient{abortRequest: request, waitChan: make(chan bool)}
	s.addOp(op)
	op.wait()
	return &Empty{}, nil
}
