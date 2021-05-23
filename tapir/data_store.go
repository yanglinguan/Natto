package tapir

import (
	"bufio"
	"sort"
	"strconv"
)

type DataStore interface {
	Write(k, v string, ver int64)
	WriteMulti(wSet map[string]string, ver int64)
	Read(k string) (string, int64)
	ReadMulti(kList []string) map[string]*VerVal
	GetLatestVersion(k string) int64

	// For testing
	LogData(w *bufio.Writer)
}

type VersionStore struct {
	kv map[string]*VersionData // key --> versioned data
}

func (s *VersionStore) LogData(w *bufio.Writer) {
	kList := make([]string, 0, len(s.kv))
	for k, _ := range s.kv {
		kList = append(kList, k)
	}
	sort.Strings(kList)
	for _, k := range kList {
		w.WriteString(k + " : ")
		vv := s.kv[k].getAllVal()
		vList := make([]int, 0, len(vv))
		for ver, _ := range vv {
			vList = append(vList, int(ver))
		}
		sort.Ints(vList)
		for _, ver := range vList {
			w.WriteString("(" + strconv.Itoa(ver) + " " + vv[int64(ver)] + "),")
		}
		w.WriteString("\n")
	}
	w.Flush()
}

func NewVersionStore() DataStore {
	s := &VersionStore{
		kv: make(map[string]*VersionData),
	}
	return s
}

func (s *VersionStore) Write(k, v string, ver int64) {
	if _, ok := s.kv[k]; !ok {
		s.kv[k] = NewVersionData()
	}
	vv, _ := s.kv[k]
	vv.putVal(v, ver)
}

func (s *VersionStore) WriteMulti(wSet map[string]string, ver int64) {
	for k, v := range wSet {
		s.Write(k, v, ver)
	}
}

func (s *VersionStore) Read(k string) (string, int64) {
	if vv, ok := s.kv[k]; ok {
		return vv.getLatestVal()
	}
	return "", TAPIR_VERSION_INVALID

}

func (s *VersionStore) ReadMulti(kList []string) map[string]*VerVal {
	ret := make(map[string]*VerVal)
	for _, k := range kList {
		val, ver := s.Read(k)
		ret[k] = &VerVal{Ver: ver, Val: val}
	}
	return ret
}

func (s *VersionStore) GetLatestVersion(k string) int64 {
	if vv, ok := s.kv[k]; ok {
		return vv.getLatestVersion()
	}
	return TAPIR_VERSION_INVALID
}

type VersionData struct {
	latestV int64            // latest version (timestamp)
	verVal  map[int64]string // version (timestamp) --> value
}

func NewVersionData() *VersionData {
	vv := &VersionData{
		latestV: TAPIR_VERSION_INVALID,
		verVal:  make(map[int64]string),
	}
	return vv
}

func (vv *VersionData) putVal(val string, ver int64) {
	vv.verVal[ver] = val
	if ver > vv.latestV {
		vv.latestV = ver
	}
}

func (vv *VersionData) getLatestVersion() int64 {
	return vv.latestV
}

func (vv *VersionData) getLatestVal() (string, int64) {
	v, ok := vv.verVal[vv.latestV]
	if !ok {
		logger.Fatalf("No value on latest version = %d", vv.latestV)
	}
	return v, vv.latestV
}

func (vv *VersionData) getVal(ver int64) (string, bool) {
	v, ok := vv.verVal[ver]
	return v, ok
}

func (vv *VersionData) getAllVal() map[int64]string {
	return vv.verVal
}
