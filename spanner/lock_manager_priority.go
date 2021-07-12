package spanner

import (
	"github.com/sirupsen/logrus"
)

type lockManagerPriority struct {
	keys map[string]*lockInfo
}

func newLockManagerPriority() *lockManagerPriority {
	lm := &lockManagerPriority{keys: make(map[string]*lockInfo)}
	return lm
}

// returns lock info for key. if not exist, create a lock info
func (lm *lockManagerPriority) createLockInfo(key string) *lockInfo {
	if _, exist := lm.keys[key]; !exist {
		lm.keys[key] = &lockInfo{
			readers: make(map[string]*transaction),
			writer:  nil,
			pq:      NewPriorityQueue(),
		}
	}
	return lm.keys[key]
}

func (lm *lockManagerPriority) lockRelease(txn *transaction, key string) {
	lockInfo := lm.createLockInfo(key)
	if _, exist := lockInfo.readers[txn.txnId]; !exist &&
		(lockInfo.writer != nil && lockInfo.writer.txnId != txn.txnId) {
		logrus.Debugf("txn %v does not hold key %v", txn.txnId, key)
		return
	}
	logrus.Debugf("txn %v release key %v", txn.txnId, key)
	delete(lockInfo.readers, txn.txnId)
	if lockInfo.writer != nil && lockInfo.writer.txnId == txn.txnId {
		lockInfo.writer = nil
	}

	for lockInfo.pq.Len() != 0 {
		waitTxn := lockInfo.pq.Peek()
		logrus.Debugf("lock release txn %v wait txn %v try to acquire lock of key %v",
			txn.txnId, waitTxn.txnId, key)

		if waitTxn.Status == ABORTED {
			logrus.Debugf("lock release txn %v wait txn %v is already aborted",
				txn.txnId, waitTxn.txnId)
			lockInfo.pq.Pop()
			continue
		}
		grant := false
		if waitTxn.getWaitKey()[key] == SHARED {
			grant = lm.lockShared(waitTxn, key)
		} else {
			grant = lm.lockExclusive(waitTxn, key)
		}

		if !grant {
			logrus.Debugf("lock release txn %v wait txn %v in the queue cannot get lock of key %v",
				txn.txnId, waitTxn.txnId, key)
			break
		}
		logrus.Debugf("lock release txn %v wait txn %v in the queue get lock of key %v",
			txn.txnId, waitTxn.txnId, key)
		lockInfo.pq.Pop()
		waitTxn.removeWaitKey(key)
		if len(waitTxn.getWaitKey()) == 0 {
			logrus.Debugf("lock release txn %v wait txn %v get all locks of keys",
				txn.txnId, waitTxn.txnId)
			if waitTxn.Status == READ {
				waitTxn.replyRead()
			} else if waitTxn.Status == WRITE {
				waitTxn.prepare()
			}
		}
	}
	logrus.Debugf("txn %v lock of key %v released", txn.txnId, key)
}

func (lm *lockManagerPriority) lockUpgrade(txn *transaction, key string) bool {
	logrus.Debugf("txn %v requires to upgrade key %v", txn.txnId, key)
	lockInfo := lm.createLockInfo(key)
	readers := lockInfo.readers
	writer := lockInfo.writer
	waitToUpgrade := lockInfo.waitToUpgrade

	if waitToUpgrade != nil && waitToUpgrade.txnId != txn.txnId {
		logrus.Debugf("key %v already has wait to upgrade txn %v abort txn %v",
			waitToUpgrade.txnId, txn.txnId)
		txn.abort()
		return false
	}

	if len(readers) == 1 && writer == nil {
		logrus.Debugf("txn %v upgraded key %v", txn.txnId, key)
		delete(readers, txn.txnId)
		lockInfo.writer = txn
		lockInfo.waitToUpgrade = nil
		return true
	}
	wound := make(map[string]*transaction)
	for _, reader := range readers {
		if reader.txnId == txn.txnId {
			continue
		}
		if txn.isOlderThan(reader) {
			if reader.Status == PREPARED {
				logrus.Debugf("key %v reader %v is already prepared %v abort txn %v",
					key, reader.txnId, txn.txnId)
				txn.abort()
				return false
			} else {
				wound[reader.txnId] = reader
			}
		}
	}

	for _, reader := range wound {
		logrus.Debugf("txn %v wound reader %v of key %v", txn.txnId, reader.txnId, key)
		reader.abort()
		delete(readers, reader.txnId)
	}

	if writer == nil {
		logrus.Debugf("txn %v upgraded key %v", txn.txnId, key)
		delete(readers, txn.txnId)
		lockInfo.writer = txn
		lockInfo.waitToUpgrade = nil
		return true
	}

	if txn.isOlderThan(writer) {
		logrus.Debugf("key %v txn %v is older than writer %v", key, txn.txnId, writer.txnId)
		if writer.Status == PREPARED {
			logrus.Debugf("key %v write %v is already prepared abort txn %v", key, writer.txnId, txn.txnId)
			txn.abort()
			return false
		} else {
			logrus.Debugf("key %v txn %v upgraded abort write %v", key, txn.txnId, writer.txnId)
			lockInfo.writer = txn
			lockInfo.waitToUpgrade = nil
			delete(readers, txn.txnId)
			writer.abort()
			return true
		}
	}
	logrus.Debugf("txn %v wait to upgrade key %v", txn.txnId, key)
	if lm.pushToQueue(txn, key, EXCLUSIVE) {
		lockInfo.waitToUpgrade = txn
	}
	return false
}

func (lm *lockManagerPriority) lockExclusive(txn *transaction, key string) bool {
	lockInfo := lm.createLockInfo(key)
	// if already acquired the shared lock, upgrade to exclusive lock
	if _, exist := lockInfo.readers[txn.txnId]; exist {
		return lm.lockUpgrade(txn, key)
	}
	logrus.Debugf("txn %v requires exclusive lock of key %v", txn.txnId, key)
	readers := lockInfo.readers
	writer := lockInfo.writer
	wound := make([]*transaction, 0)
	// if there is no reader and writer, txn grants the exclusive lock
	if len(readers) == 0 && writer == nil {
		logrus.Debugf("txn %v acquired exclusive lock of key %v", txn.txnId, key)
		lockInfo.writer = txn
		return true
	}

	for _, reader := range readers {
		if txn.isOlderThan(reader) {
			logrus.Debugf("key %v txn %v is older than reader %v", key, txn.txnId, reader.txnId)
			if reader.Status == PREPARED {
				logrus.Debugf("key %v reader %v is already prepared abort txn %v", key, reader.txnId, txn.txnId)
				txn.abort()
				return false
			}
			wound = append(wound, reader)
		}
	}

	for _, reader := range wound {
		logrus.Debugf("key %v txn %v wound reader %v", key, txn.txnId, reader.txnId)
		reader.abort()
		delete(readers, reader.txnId)
	}

	if writer == nil {
		logrus.Debugf("txn %v acquired exclusive lock of key %v", txn.txnId, key)
		lockInfo.writer = txn
		return true
	}

	if txn.isOlderThan(writer) {
		logrus.Debugf("key %v txn %v is older than writer %v",
			key, txn.txnId, writer.txnId)
		if writer.Status == PREPARED {
			logrus.Debugf("key %v writer %v is already prepared %v abort txn %v",
				key, writer.txnId, txn.txnId)
			txn.abort()
			return false
		} else {
			logrus.Debugf("key %v txn %v wound writer %v",
				key, txn.txnId, writer.txnId)
			lockInfo.writer = txn
			writer.abort()
			return true
		}
	}
	logrus.Debugf("txn %v wait to acquire exclusive lock of key %v", txn.txnId, key)
	lm.pushToQueue(txn, key, EXCLUSIVE)
	return false
}

func (lm *lockManagerPriority) lockShared(txn *transaction, key string) bool {
	lockInfo := lm.createLockInfo(key)
	writer := lockInfo.writer
	// if there is no writer, txn grants the shared lock
	if writer == nil {
		logrus.Debugf("txn %v acquired shared lock for key %v", txn.txnId, key)
		lockInfo.readers[txn.txnId] = txn
		return true
	}

	// if txn has smaller timestamp (older) than the writer
	// if the write is already prepared then abort txn
	// otherwise abort writer
	if txn.isOlderThan(writer) {
		logrus.Debugf("shared lock key %v txn %v has older timestamp than write %v",
			key, txn.txnId, writer.txnId)
		if writer.Status == PREPARED {
			logrus.Debugf("key %v write %v already prepared abort txn %v", key, writer.txnId, txn.txnId)
			txn.abort()
			return false
		} else {
			lockInfo.readers[txn.txnId] = txn
			logrus.Debugf("key %v txn %v get shard lock wound write %v", key, writer.txnId, txn.txnId)
			writer.abort()
			return true
		}
	}
	logrus.Debugf("txn %v wait to get shard lock of key %v", txn.txnId, key)

	lm.pushToQueue(txn, key, SHARED)
	return false
}

func (lm *lockManagerPriority) pushToQueue(txn *transaction, key string, lockType LockType) bool {
	lockInfo := lm.createLockInfo(key)
	lockInfo.pq.Push(txn)
	if txn.priority {
		highTxnList := make([]*transaction, 0)
		for lockInfo.pq.Len() != 0 {
			waitTxn := lockInfo.pq.Peek()
			if waitTxn.txnId == txn.txnId {
				break
			}
			if !waitTxn.priority {
				waitTxn.abort()
			} else {
				highTxnList = append(highTxnList, waitTxn)
			}
			lockInfo.pq.Pop()
		}
		for _, t := range highTxnList {
			lockInfo.pq.Push(t)
		}
	} else {
		txnList := make([]*transaction, 0)
		for lockInfo.pq.Len() != 0 {
			waitTxn := lockInfo.pq.Peek()
			if waitTxn.txnId == txn.txnId {
				break
			}
			if waitTxn.priority {
				txn.abort()
				break
			}
			txnList = append(txnList, waitTxn)
			lockInfo.pq.Pop()
		}
		for _, t := range txnList {
			lockInfo.pq.Push(t)
		}

		if txn.Status == ABORTED {
			lockInfo.pq.Remove(txn)
			return false
		}
	}
	txn.addWaitKey(key, lockType)
	return true
}
