package server

import (
	"Carousel-GTS/rpc"
)

type ReadAndPreparePriority struct {
	*ReadAndPrepare2PL

	// include keys in other partition
	allKeys      map[string]bool
	allReadKeys  map[string]bool
	allWriteKeys map[string]bool

	otherPartitionKeys map[string]bool

	// true: there is a conflict high priority txn within. only low priority txn will selfAbort
	selfAbort bool
}

func NewReadAndPreparePriorityWithReplicatedMsg(msg *ReplicationMsg) *ReadAndPreparePriority {
	r := &ReadAndPreparePriority{
		ReadAndPrepare2PL:  NewReadAndPrepare2PLWithReplicationMsg(msg),
		allKeys:            make(map[string]bool),
		allReadKeys:        make(map[string]bool),
		allWriteKeys:       make(map[string]bool),
		otherPartitionKeys: make(map[string]bool),
	}

	return r
}

func NewReadAndPreparePriority(request *rpc.ReadAndPrepareRequest, server *Server) *ReadAndPreparePriority {
	r := &ReadAndPreparePriority{
		ReadAndPrepare2PL:  NewReadAndPrepareLock2PL(request),
		allKeys:            make(map[string]bool),
		allReadKeys:        make(map[string]bool),
		allWriteKeys:       make(map[string]bool),
		otherPartitionKeys: make(map[string]bool),
	}

	if server.config.IsEarlyAbort() || server.config.IsOptimisticReorder() {
		r.keyMap = make(map[string]bool)
		r.readKeyList = make(map[string]bool)
		r.writeKeyList = make(map[string]bool)

		r.processKey(request.Txn.ReadKeyList, server, READ)
		r.processKey(request.Txn.WriteKeyList, server, WRITE)
	}

	return r
}

func (o *ReadAndPreparePriority) processKey(keys []string, server *Server, keyType KeyType) {
	for _, key := range keys {
		o.allKeys[key] = true
		if keyType == WRITE {
			o.allWriteKeys[key] = false
		} else if keyType == READ {
			o.allReadKeys[key] = false
		}

		if !server.storage.HasKey(key) {
			o.otherPartitionKeys[key] = true
			continue
		}

		if keyType == WRITE {
			o.writeKeyList[key] = false
		} else if keyType == READ {
			o.readKeyList[key] = false
		}
		o.keyMap[key] = true
	}
}

func (o *ReadAndPreparePriority) GetAllKeys() map[string]bool {
	return o.allKeys
}

func (o *ReadAndPreparePriority) GetAllWriteKeys() map[string]bool {
	return o.allWriteKeys
}

func (o *ReadAndPreparePriority) GetAllReadKeys() map[string]bool {
	return o.allReadKeys
}

func (o *ReadAndPreparePriority) setSelfAbort() {
	o.selfAbort = true
}

func (o *ReadAndPreparePriority) IsSelfAbort() bool {
	return o.selfAbort
}
