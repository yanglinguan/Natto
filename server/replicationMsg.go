package server

import (
	"Carousel-GTS/rpc"
)

type ReplicationMsgType int32

// Message type
const (
	PrepareResultMsg ReplicationMsgType = iota
	CommitResultMsg
	WriteDataMsg
)

type ReplicationMsg struct {
	TxnId                   string
	Status                  TxnStatus
	MsgType                 ReplicationMsgType
	WriteData               []*rpc.KeyValue
	WriteDataFromLeader     []bool
	PreparedReadKeyVersion  []*rpc.KeyVersion
	PreparedWriteKeyVersion []*rpc.KeyVersion
	Conditions              []string
	Forward                 []string
	IsFastPathSuccess       bool
	IsFromCoordinator       bool
	TotalCommit             int
	HighPriority            bool
	ReadTS                  int64
	WriteTS                 int64
}
