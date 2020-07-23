package server

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
)

func (s *Storage) print() {
	log.Debugf("total commit %v committed %v", s.totalCommit, s.committed)
	if s.waitPrintStatusRequest != nil && s.totalCommit == s.committed {
		s.kvStore.finalWaitStateCheck()
		s.printCommitOrder()
		s.printModifiedData()
		s.printAllTxn()
		s.server.coordinator.print()
		s.waitPrintStatusRequest.UnblockClient()
	}
}

func (s Storage) printAllTxn() {
	fName := fmt.Sprintf("s%v_%v_allTxn.log", s.server.serverId, s.server.IsLeader())
	file, err := os.Create(fName)
	if err != nil || file == nil {
		log.Fatal("Fails to create log file: statistic.log")
		return
	}
	for txnId, info := range s.txnStore {
		line := ""
		if s.server.IsLeader() {
			line = fmt.Sprintf("%v %v %v %v %v %v %v %v %v %v %v\n",
				txnId,
				info.startTime,
				info.maxQueueLen,
				info.preparedTime.Sub(info.startTime).Nanoseconds(),
				info.commitTime.Sub(info.preparedTime).Nanoseconds(),
				info.commitTime.Sub(info.startTime).Nanoseconds(),
				info.canReorder,
				info.isFastPrepare,
				//info.readAndPrepareRequestOp.prepareResult.Timestamp,
				info.hasWaitingButNoWriteReadConflict,
				info.commitOrder,
				info.selfAbort,
			)
		} else {
			if info == nil {
				log.Warnf("txn %v info %v total commit %v, len %v",
					txnId, info, s.totalCommit, len(s.txnStore))
			} else {
				line = fmt.Sprintf("%v %v %v %v %v %v %v\n",
					txnId,
					info.waitingTxnKey,
					info.waitingTxnDep,
					info.canReorder,
					info.isFastPrepare,
					0,
					info.hasWaitingButNoWriteReadConflict,
				)
			}
		}
		_, err = file.WriteString(line)
		if err != nil {
			log.Fatalf("Cannot write to file %v", err)
		}
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close file %v", err)
	}
}

func (s Storage) printCommitOrder() {
	txnInfo := make([]*TxnInfo, s.committed)
	txnId := make([]string, s.committed)
	for id, info := range s.txnStore {
		if info.status == COMMIT {
			txnInfo[info.commitOrder] = info
			txnId[info.commitOrder] = id
		}
	}
	fName := fmt.Sprintf("s%v_%v_commitOrder.log", s.server.serverId, s.server.IsLeader())
	file, err := os.Create(fName)
	if err != nil || file == nil {
		log.Fatal("Fails to create log file: statistic.log")
		return
	}

	for i, info := range txnInfo {
		line := ""
		if s.server.IsLeader() {
			line = fmt.Sprintf("%v %v %v %v %v %v %v %v %v %v %v %v\n",
				txnId[i],
				info.waitingTxnKey,
				info.waitingTxnDep,
				info.preparedTime.Sub(info.startTime).Nanoseconds(),
				info.commitTime.Sub(info.preparedTime).Nanoseconds(),
				info.commitTime.Sub(info.startTime).Nanoseconds(),
				info.canReorder,
				info.isFastPrepare,
				info.readAndPrepareRequestOp.GetTimestamp(),
				info.hasWaitingButNoWriteReadConflict,
				info.commitOrder,
				info.selfAbort,
			)
		} else {
			if info == nil {
				log.Warnf("txn %v info %v order %v total commit %v, len %v",
					txnId[i], info, i, s.totalCommit, len(s.txnStore))
			} else {

				line = fmt.Sprintf("%v %v %v %v %v %v %v\n",
					txnId[i],
					info.waitingTxnKey,
					info.waitingTxnDep,
					info.canReorder,
					info.isFastPrepare,
					0,
					info.hasWaitingButNoWriteReadConflict,
				)
			}
		}
		_, err = file.WriteString(line)
		if err != nil {
			log.Fatalf("Cannot write to file %v", err)
		}
	}

	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close file %v", err)
	}
}

func (s Storage) printModifiedData() {
	fName := fmt.Sprintf("s%v_%v_db.log", s.server.serverId, s.server.partitionId)
	s.kvStore.printModifiedData(fName)
}
