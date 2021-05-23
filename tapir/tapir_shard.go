package tapir

// Sharding manager: maps keys to partitions
type TapirShardMngr interface {
	//Returns paritiontId --> read key list
	MapKeyListToShard(readKeyList []string) map[int32][]string
	//Returns partitionId --> write key --> value
	MapKeySetToShard(writeKeySet map[string]string) map[int32]map[string]string
}

type firstLetterShardMngr struct {
	pNum int32 // number of partitions
}

func NewFirstLetterShardMngr(n int32) TapirShardMngr {
	return &firstLetterShardMngr{pNum: n}
}

func (m *firstLetterShardMngr) mapKeyToPId(k string) int32 {
	if len(k) == 0 {
		return 0 // TODO fix this
	}
	pId := int32(k[0]) % m.pNum
	return pId
}

func (m *firstLetterShardMngr) MapKeyListToShard(readKeyList []string) map[int32][]string {
	ret := make(map[int32][]string)
	for _, k := range readKeyList {
		pId := m.mapKeyToPId(k)
		if _, ok := ret[pId]; !ok {
			ret[pId] = make([]string, 0)
		}
		ret[pId] = append(ret[pId], k)
	}
	return ret
}

func (m *firstLetterShardMngr) MapKeySetToShard(
	writeKeySet map[string]string,
) map[int32]map[string]string {
	ret := make(map[int32]map[string]string)
	for k, v := range writeKeySet {
		pId := m.mapKeyToPId(k)
		if _, ok := ret[pId]; !ok {
			ret[pId] = make(map[string]string)
		}
		ret[pId][k] = v
	}
	return ret
}
