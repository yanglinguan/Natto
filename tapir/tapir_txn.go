package tapir

// Not thread-safe
type Txn struct {
	id         string
	pRwSet     map[int32]*RwSet // partitionId --> read and write set
	isComplete bool             // true: not allow any more reads/writes
}

func NewTxn(id string) *Txn {
	return &Txn{
		id:         id,
		pRwSet:     make(map[int32]*RwSet),
		isComplete: false,
	}
}

func (t *Txn) getId() string {
	return t.id
}

func (t *Txn) complete() {
	if t.isComplete {
		logger.Fatalf("Txn id = %s has completed.", t.id)
	}
	t.isComplete = true
}

func (t *Txn) getRwSet() map[int32]*RwSet {
	return t.pRwSet
}

func (t *Txn) getOrCreateRwSet(pId int32) *RwSet {
	if _, ok := t.pRwSet[pId]; !ok {
		t.pRwSet[pId] = NewRwSet()
	}
	return t.pRwSet[pId]
}

func (t *Txn) writeData(pId int32, wData map[string]string) {
	if t.isComplete {
		logger.Fatalf("Txn id = %s has completed reads & writes. Write pId = %d %v",
			t.id, pId, wData)
	}

	rwSet := t.getOrCreateRwSet(pId)
	for k, v := range wData {
		rwSet.Write(k, v)
	}
}

func (t *Txn) addReadResult(pId int32, result map[string]*VerVal) {
	if t.isComplete {
		logger.Fatalf("Txn id = %s has completed reads & writes. Add read results pId = %d %v",
			t.id, pId, result)
	}

	rwSet := t.getOrCreateRwSet(pId)
	for k, vv := range result {
		rwSet.AddReadResult(k, vv.Val, vv.Ver)
	}
}

func (t *Txn) readBufferData(pId int32, k string) (string, bool) {
	if t.isComplete {
		logger.Fatalf("Txn id = %s has completed reads & writes. Read local buffer pId = %d %v",
			t.id, pId, k)
	}

	if rwSet, ok := t.pRwSet[pId]; ok {
		return rwSet.Read(k)
	}
	return "", false
}

func (t *Txn) getPartitionRwSet(pId int32) (bool, map[string]int64, map[string]string) {
	if rwSet, ok := t.pRwSet[pId]; ok {
		return true, rwSet.readSet, rwSet.writeSet
	}
	return false, nil, nil
}

func (t *Txn) clear() {
	t.pRwSet = nil
}

func (t *Txn) clearReadVal() {
	for _, rwSet := range t.pRwSet {
		rwSet.clearReadVal()
	}
}

type RwSet struct {
	writeSet   map[string]string // write set (key --> write value)
	readSet    map[string]int64  // read set (key --> read timestamp)
	readResult map[string]string // read results (key --> read value)
}

func NewRwSet() *RwSet {
	return &RwSet{
		writeSet:   make(map[string]string),
		readSet:    make(map[string]int64),
		readResult: make(map[string]string),
	}
}

func (rw *RwSet) GetWriteSet() map[string]string {
	return rw.writeSet
}

func (rw *RwSet) GetReadSet() map[string]int64 {
	return rw.readSet
}

func (rw *RwSet) Write(k, v string) {
	rw.writeSet[k] = v
}

func (rw *RwSet) Read(k string) (string, bool) {
	// Reads own write
	if v, ok := rw.writeSet[k]; ok {
		return v, true
	}
	// Consistent reads
	if v, ok := rw.readResult[k]; ok {
		return v, true
	}
	return "", false
}

func (rw *RwSet) AddReadResult(k, val string, ver int64) {
	rw.readSet[k] = ver
	rw.readResult[k] = val
}

func (rw *RwSet) clearReadVal() {
	rw.readResult = nil
}
