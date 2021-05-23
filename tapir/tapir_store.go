package tapir

import (
	"bufio"
	"math"
	"os"
	"sort"
	"strconv"
)

type TapirStore struct {
	id    string
	store DataStore // data store

	// committed txns
	commitTable map[string]int64 // id --> timestamp
	// aborted txns
	abortTable map[string]int64 // id --> timestamp

	// prepared txns (i.e., preparedList)
	preparedTable map[string]int64 // prepared txns, txnId --> txn timestamp

	//// Helper data structures
	// prepared write key --> (prepared tId --> txn timestamp)
	preparedWriteKeyTable map[string]map[string]int64
	// prepared read key --> (prepared tId --> txn timestamp)
	preparedReadKeyTable map[string]map[string]int64
}

func NewTapirStore(
	id string,
	s DataStore,
) *TapirStore {
	t := &TapirStore{
		id:                    id,
		store:                 s,
		commitTable:           make(map[string]int64),
		abortTable:            make(map[string]int64),
		preparedTable:         make(map[string]int64),
		preparedWriteKeyTable: make(map[string]map[string]int64),
		preparedReadKeyTable:  make(map[string]map[string]int64),
	}
	return t
}

func (t *TapirStore) InitData(keyList []string, val string) {
	for _, k := range keyList {
		t.store.Write(k, val, TAPIR_VERSION_INIT)
	}
}

func (t *TapirStore) InitSmallBankData(
	keys []string, // customer/account IDs
	cFlag, sFlag string, // flags for checking and savings accounts for each customer
	cB, sB string, // initialized value for checkinga and savings accounts
) {
	for _, key := range keys {
		t.store.Write(key+cFlag, cB, TAPIR_VERSION_INIT) // checking account
		t.store.Write(key+sFlag, sB, TAPIR_VERSION_INIT) // savings account
	}
}

func (t *TapirStore) ExecUnLog(op interface{}) interface{} {
	switch op.(type) {
	case *ReadOp:
		o := op.(*ReadOp)
		return t.ReadMulti(o.Key)
	default:
		logger.Fatalf("Unknown unlog operation: %v", op)
	}
	return nil
}

func (t *TapirStore) ExecConsensus(op interface{}) interface{} {
	switch op.(type) {
	case *PrepareOp:
		o := op.(*PrepareOp)
		state, timestamp := t.Prepare(o.Id, o.T, o.Write, o.Read)
		logger.Debugf("Prepare txnId = %s result = %d %d", o.Id, state, timestamp)
		return &PrepareOpRet{State: state, T: timestamp}
	default:
		logger.Fatalf("Unknown consensus operation: %v", op)
	}
	return nil
}

func (t *TapirStore) ExecInconsistent(op interface{}) {
	switch op.(type) {
	case *CommitOp:
		o := op.(*CommitOp)
		if o.IsC {
			t.Commit(o.Id, o.T, o.Write, o.Read)
		} else {
			t.Abort(o.Id, o.T, o.Write, o.Read)
		}
	default:
		logger.Fatalf("Unknown inconsistent operation: %v", op)
	}
}

func (t *TapirStore) ReadMulti(kList []string) *ReadOpRet {
	ret := t.store.ReadMulti(kList)
	return &ReadOpRet{Ret: ret}
}

func (t *TapirStore) Prepare(
	tId string, // txn id
	timestamp int64, // txn timestamp
	wSet map[string]string, // write set
	rSet map[string]int64, // read set
) (int, int64) {
	//if commitT, ok := t.commitTable[tId]; ok {
	if _, ok := t.commitTable[tId]; ok {
		// txn has been committed
		//return TAPIR_TXN_PREPARE_OK, commitT
		return TAPIR_TXN_PREPARE_OK, TAPIR_VERSION_INVALID
	}
	//if abortT, ok := t.abortTable[tId]; ok {
	if _, ok := t.abortTable[tId]; ok {
		// txn has been aborted
		//return TAPIR_TXN_ABORT, abortT
		return TAPIR_TXN_ABORT, TAPIR_VERSION_INVALID
	}

	if pT, ok := t.preparedTable[tId]; ok {
		if pT >= timestamp {
			//return TAPIR_TXN_PREPARE_OK, timestamp
			return TAPIR_TXN_PREPARE_OK, TAPIR_VERSION_INVALID
		} else {
			// A retried prepare request that has a larger timestamp, prepare again
			t.delPreparedTxn(tId, wSet, rSet)
		}
	}

	// TAPIR-OCC-Check
	// Read keys
	for rKey, rT := range rSet {
		latestT := t.store.GetLatestVersion(rKey)
		if latestT == TAPIR_VERSION_INVALID {
			// Read key does not exist
			//return TAPIR_TXN_ABORT, latestT
			return TAPIR_TXN_ABORT, TAPIR_VERSION_INVALID
		}
		if rT < latestT {
			//return TAPIR_TXN_ABORT, latestT
			return TAPIR_TXN_ABORT, TAPIR_VERSION_INVALID
		}
		if minT, ok := t.getMinTimestampPreparedWriteKey(rKey); ok {
			if rT < minT {
				//return TAPIR_TXN_ABSTAIN, minT
				return TAPIR_TXN_ABSTAIN, TAPIR_VERSION_INVALID
			}
		}
	}
	// Write keys
	for wKey, _ := range wSet {
		if maxT, ok := t.getMaxTimestampPreparedReadKey(wKey); ok {
			if timestamp < maxT {
				return TAPIR_TXN_RETRY, maxT
			}
		}
		latestT := t.store.GetLatestVersion(wKey)
		if timestamp < latestT {
			return TAPIR_TXN_RETRY, latestT
		}
	}

	t.addPreparedTxn(tId, timestamp, wSet, rSet)

	//return TAPIR_TXN_PREPARE_OK, timestamp
	return TAPIR_TXN_PREPARE_OK, TAPIR_VERSION_INVALID
}

func (t *TapirStore) Commit(
	tId string, // txn id
	timestamp int64, // txn timestamp
	wSet map[string]string, // write set
	rSet map[string]int64, // read set
) {
	logger.Debugf("Commit txnId = %s timestamp = %d wSet = %v", tId, timestamp, wSet)
	t.commitTable[tId] = timestamp
	t.store.WriteMulti(wSet, timestamp) // Update data store
	t.delPreparedTxn(tId, wSet, rSet)
}

func (t *TapirStore) Abort(
	tId string, // txn id
	timestamp int64, // txn timestamp
	wSet map[string]string, // write set
	rSet map[string]int64, // read set
) {
	logger.Debugf("Abort txnId = %s timestamp = %d", tId, timestamp)
	t.abortTable[tId] = timestamp
	t.delPreparedTxn(tId, wSet, rSet)
}

// Helper functions
func (t *TapirStore) getMinTimestampPreparedWriteKey(key string) (int64, bool) {
	var minT int64 = math.MaxInt64
	if pTxnTable, ok := t.preparedWriteKeyTable[key]; ok && len(pTxnTable) > 0 {
		for _, pT := range pTxnTable {
			if pT < minT {
				minT = pT
			}
		}
		return minT, true
	}
	return TAPIR_VERSION_INVALID, false
}

func (t *TapirStore) getMaxTimestampPreparedReadKey(key string) (int64, bool) {
	var maxT int64 = 0
	if pTxnTable, ok := t.preparedReadKeyTable[key]; ok && len(pTxnTable) > 0 {
		for _, pT := range pTxnTable {
			if pT > maxT {
				maxT = pT
			}
		}
		return maxT, true
	}
	return TAPIR_VERSION_INVALID, false
}

func (t *TapirStore) addPreparedTxn(
	tId string,
	timestamp int64,
	wSet map[string]string,
	rSet map[string]int64,
) {
	t.preparedTable[tId] = timestamp
	// Record write keys
	for wKey, _ := range wSet {
		if _, ok := t.preparedWriteKeyTable[wKey]; !ok {
			t.preparedWriteKeyTable[wKey] = make(map[string]int64)
		}
		t.preparedWriteKeyTable[wKey][tId] = timestamp
	}
	// Record read keys
	for rKey, _ := range rSet {
		if _, ok := t.preparedReadKeyTable[rKey]; !ok {
			t.preparedReadKeyTable[rKey] = make(map[string]int64)
		}
		t.preparedReadKeyTable[rKey][tId] = timestamp
	}
}

func (t *TapirStore) delPreparedTxn(tId string, wSet map[string]string, rSet map[string]int64) {
	delete(t.preparedTable, tId)
	for wKey, _ := range wSet {
		if tTable, ok := t.preparedWriteKeyTable[wKey]; ok {
			delete(tTable, tId)
			if len(tTable) == 0 {
				delete(t.preparedWriteKeyTable, wKey)
			}
		}
	}
	for rKey, _ := range rSet {
		if tTable, ok := t.preparedReadKeyTable[rKey]; ok {
			delete(tTable, tId)
			if len(tTable) == 0 {
				delete(t.preparedReadKeyTable, rKey)
			}
		}
	}
}

// For testing
func (t *TapirStore) Test() {
	// Print testing log
	t.logCommitTxn()
	t.logAbortTxn()
	t.logPreparedTxn()
	t.logPreparedReadKeys()
	t.logPreparedWriteKeys()
	t.logData()
}

func (t *TapirStore) logCommitTxn() {
	logger.Infof("# of committed txns = %d", len(t.commitTable))
	if len(t.commitTable) > 0 {
		w, f := createLog("server-"+t.id+"-commit.log", t.id)
		tIdList := getSortedTIdList(t.commitTable)
		logTable(w, tIdList, t.commitTable)
		w.Flush()
		f.Close()
	}
}

func (t *TapirStore) logAbortTxn() {
	logger.Infof("# of abort txns = %d", len(t.abortTable))
	if len(t.abortTable) > 0 {
		w, f := createLog("server-"+t.id+"-abort.log", t.id)
		tIdList := getSortedTIdList(t.abortTable)
		logTable(w, tIdList, t.abortTable)
		w.Flush()
		f.Close()
	}
}

func (t *TapirStore) logPreparedTxn() {
	logger.Infof("# of prepared txns = %d", len(t.preparedTable))
	if len(t.preparedTable) > 0 {
		w, f := createLog("server-"+t.id+"-prepare.log", t.id)
		tIdList := getSortedTIdList(t.preparedTable)
		logTable(w, tIdList, t.preparedTable)
		w.Flush()
		f.Close()
	}
}

func (t *TapirStore) logPreparedReadKeys() {
	logger.Infof("# of prepared read keys = %d", len(t.preparedReadKeyTable))
	if len(t.preparedReadKeyTable) > 0 {
		w, f := createLog("server-"+t.id+"-prepareRead.log", t.id)
		tIdList := getSortedKeyList(t.preparedReadKeyTable)
		logNestedTable(w, tIdList, t.preparedReadKeyTable)
		w.Flush()
		f.Close()
	}
}

func (t *TapirStore) logPreparedWriteKeys() {
	logger.Infof("# of prepared write keys = %d", len(t.preparedWriteKeyTable))
	if len(t.preparedWriteKeyTable) > 0 {
		w, f := createLog("server-"+t.id+"-prepareWrite.log", t.id)
		tIdList := getSortedKeyList(t.preparedWriteKeyTable)
		logNestedTable(w, tIdList, t.preparedWriteKeyTable)
		w.Flush()
		f.Close()
	}
}

func (t *TapirStore) logData() {
	w, f := createLog("server-"+t.id+"-data.log", t.id)
	t.store.LogData(w)
	w.Flush()
	f.Close()
}

func getSortedTIdList(t map[string]int64) []string {
	tIdList := make([]string, 0, len(t))
	for tId, _ := range t {
		tIdList = append(tIdList, tId)
	}
	sort.Strings(tIdList)
	return tIdList
}

func getSortedKeyList(t map[string]map[string]int64) []string {
	kList := make([]string, 0, len(t))
	for k, _ := range t {
		kList = append(kList, k)
	}
	sort.Strings(kList)
	return kList
}

func createLog(n, sId string) (*bufio.Writer, *os.File) {
	f, err := os.Create(n)
	if err != nil {
		logger.Fatalf("Fails to create log file %s for server Id = %s", n, sId)
	}
	w := bufio.NewWriter(f)
	return w, f
}

func logTable(w *bufio.Writer, kList []string, t map[string]int64) {
	for _, k := range kList {
		w.WriteString(k + "->" + strconv.FormatInt(t[k], 10) + "\n")
	}
	w.Flush()
}

func logNestedTable(w *bufio.Writer, kList []string, t map[string]map[string]int64) {
	for _, k := range kList {
		w.WriteString(k + " : ")
		tIdList := getSortedTIdList(t[k])
		for _, tId := range tIdList {
			w.WriteString("(" + tId + "->" + strconv.FormatInt(t[k][tId], 10) + "),")
		}
		w.WriteString("\n")
	}
	w.Flush()
}
