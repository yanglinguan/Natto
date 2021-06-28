package spanner

import "strconv"

type kvStore struct {
	keys map[string]*ValVer
}

func newKVStore() *kvStore {
	kv := &kvStore{keys: make(map[string]*ValVer)}
	return kv
}

func (kv *kvStore) initKeyValue(key string, val string) {
	kv.keys[key] = &ValVer{
		Val: val,
		Ver: "",
	}
}

func (kv *kvStore) loadKeys(keys []string) {
	for _, key := range keys {
		kv.initKeyValue(key, key)
	}
}

func (kv kvStore) read(keys []string) []*ValVer {
	result := make([]*ValVer, len(keys))
	for i, key := range keys {
		result[i] = kv.keys[key]
	}
	return result
}

func (kv *kvStore) write(keyVal map[string]string, ts int64, cId int64) {
	for key, val := range keyVal {
		kv.keys[key].Val = val
		kv.keys[key].Ver = strconv.FormatInt(ts, 10) + "-" + strconv.FormatInt(cId, 10)
	}
}

func (kv *kvStore) versionCheck(key string, version string) bool {
	return kv.keys[key].Ver == version
}
