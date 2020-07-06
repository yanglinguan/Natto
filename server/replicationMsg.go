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
	PreparedReadKeyVersion  []*rpc.KeyVersion
	PreparedWriteKeyVersion []*rpc.KeyVersion
	Conditions              []int32
	IsFastPathSuccess       bool
	IsFromCoordinator       bool
	TotalCommit             int
	HighPriority            bool
}
