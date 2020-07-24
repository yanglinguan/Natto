package server

type ApplyPrepareReplicationMsgTO struct {
	*ApplyPrepareReplicationMsgOCC
}

func NewApplyPrepareReplicationMsgTO(msg *ReplicationMsg) *ApplyPrepareReplicationMsgTO {
	return &ApplyPrepareReplicationMsgTO{NewApplyPrepareReplicationMsgOCC(msg)}
}
