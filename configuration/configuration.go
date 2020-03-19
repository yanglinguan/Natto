package configuration

import (
	"Carousel-GTS/utils"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"math/rand"
	"time"
)

type ServerMode int

const (
	OCC ServerMode = iota
	GTS
	GtsDepGraph
	GTSReorder
	OCCReadOnly
)

type WorkLoad int

const (
	YCSBT WorkLoad = iota
	ONETXN
	RETWIS
)

type RetryMode int

const (
	EXP RetryMode = iota
	CONST
	OFF
)

type Configuration interface {
	GetServerAddressByServerId(serverId string) string
	GetServerIdByPartitionId(partitionId int) string
	GetServerAddressMap() map[string]string

	GetPartitionIdByServerId(serverId string) int

	GetServerMode() ServerMode
	GetKeyListByPartitionId(partitionId int) []string
	GetPartitionIdByKey(key string) int
	GetDataCenterIdByServerId(serverId string) string
	GetDataCenterIdByClientId(clientId int) string
	GetMaxDelay(clientDCId string, dcIds []string) time.Duration
	GetServerListByDataCenterId(dataCenterId string) []string
	GetKeyNum() int64
	GetDelay() time.Duration
	GetConnectionPoolSize() int
	GetKeySize() int
	GetTxnSize() int
	GetTxnRate() int
	GetExpDuration() time.Duration
	GetOpenLoop() bool
	GetZipfAlpha() float64
	GetWorkLoad() WorkLoad
	GetTotalTxn() int
	GetTotalClient() int
	GetRandSeed() int64
	GetQueueLen() int
	GetRetryInterval() time.Duration
	GetRetryMode() RetryMode
	GetMaxRetry() int64
	GetRetryMaxSlot() int64

	GetAddUserRatio() int
	GetFollowUnfollowRatio() int
	GetPostTweetRatio() int
	GetLoadTimelineRatio() int
}

type FileConfiguration struct {
	// serverId -> server Address (ip:port)
	servers    map[string]string
	clients    int
	partitions [][]string

	serverToPartitionMap map[string]int

	serverMode ServerMode

	keys   [][]string
	keyNum int64

	// dataCenterId -> (dataCenterId -> distance)
	dataCenterDistance map[string]map[string]time.Duration
	// serverId -> dataCenterId
	serverToDataCenterId map[string]string
	// dataCenterId -> serverId
	dataCenterIdToServerIdList map[string][]string

	// clientId -> dataCenterId
	clientToDataCenterId map[int]string

	delay         time.Duration
	poolSize      int
	keySize       int
	txnSize       int
	totalTxn      int
	openLoop      bool
	duration      time.Duration // second
	txnRate       int
	zipfAlpha     float64
	workload      WorkLoad
	seed          int64
	queueLen      int
	retryInterval time.Duration // millisecond
	retryMode     RetryMode
	maxRetry      int64 // -1: retry until commit, otherwise only retry maxRetry time
	maxSlot       int64

	addUserRatio        int
	followUnfollowRatio int
	postTweetRatio      int
	loadTimelineRatio   int
}

func NewFileConfiguration(filePath string) *FileConfiguration {
	c := &FileConfiguration{
		servers:                    make(map[string]string),
		clients:                    0,
		partitions:                 make([][]string, 0),
		serverToPartitionMap:       make(map[string]int),
		serverMode:                 0,
		keys:                       make([][]string, 0),
		keyNum:                     0,
		dataCenterDistance:         make(map[string]map[string]time.Duration),
		serverToDataCenterId:       make(map[string]string),
		dataCenterIdToServerIdList: make(map[string][]string),
		clientToDataCenterId:       make(map[int]string),
	}
	c.loadFile(filePath)
	return c
}

func (f *FileConfiguration) loadFile(configFilePath string) {
	data, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		log.Fatalf("cannot read the configuration file: err %s", err)
	}
	config := make(map[string]interface{})
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatal("cannot parse the json file: err %s", err)
	}

	f.loadServers(config["servers"].(map[string]interface{}))
	f.loadClients(config["clients"].(map[string]interface{}))
	f.loadPartitions(config["partitions"].([]interface{}))
	f.loadExperiment(config["experiment"].(map[string]interface{}))

	f.loadKey()
}

func (f *FileConfiguration) loadServers(config map[string]interface{}) {
	for sId, kv := range config {
		addr := kv.(map[string]interface{})
		address := addr["ip"].(string) + ":" + addr["port"].(string)
		f.servers[sId] = address
		dcId := addr["dataCenterId"].(string)
		f.serverToDataCenterId[sId] = dcId
		if _, exist := f.dataCenterIdToServerIdList[dcId]; !exist {
			f.dataCenterIdToServerIdList[dcId] = make([]string, 0)
		}
		f.dataCenterIdToServerIdList[dcId] = append(f.dataCenterIdToServerIdList[dcId], sId)
	}
}

func (f *FileConfiguration) loadClients(config map[string]interface{}) {
	f.clients = int(config["nums"].(float64))
	machines := config["machines"].([]interface{})
	totalMachines := len(machines)
	for id := 0; id < f.clients; id++ {
		idx := id % totalMachines
		machine := machines[idx].(map[string]interface{})
		f.clientToDataCenterId[id] = machine["dataCenterId"].(string)
	}
}

func (f *FileConfiguration) loadPartitions(config []interface{}) {
	for pId, servers := range config {
		sList := servers.([]interface{})
		f.partitions = append(f.partitions, make([]string, 0))
		for _, sId := range sList {
			f.partitions[pId] = append(f.partitions[pId], sId.(string))
			f.serverToPartitionMap[sId.(string)] = pId
		}
	}
}

func (f *FileConfiguration) loadDataCenterDistance(config map[string]interface{}) {
	var err error
	for dcId, m := range config {
		f.dataCenterDistance[dcId] = make(map[string]time.Duration)
		f.dataCenterDistance[dcId][dcId], err = time.ParseDuration("0ms")
		if err != nil {
			log.Fatalf("Sets local delay fails: %v", err)
		}
		for id, dis := range m.(map[string]interface{}) {
			f.dataCenterDistance[dcId][id], err = time.ParseDuration(dis.(string))
			if err != nil {
				log.Fatalf("Sets delay (%v, %v) fails: %v", dcId, id, err)
			}
		}
	}
}

func (f *FileConfiguration) loadExperiment(config map[string]interface{}) {
	var err error
	for key, v := range config {
		if key == "mode" {
			mode := v.(string)
			if mode == "occ" {
				f.serverMode = OCC
			} else if mode == "gts" {
				f.serverMode = GTS
			} else if mode == "gts_dep_graph" {
				f.serverMode = GtsDepGraph
			} else if mode == "gts_reorder" {
				f.serverMode = GTSReorder
			} else if mode == "occ_readOnly" {
				f.serverMode = OCCReadOnly
			}
		} else if key == "totalKey" {
			keyNum := v.(float64)
			f.keyNum = int64(keyNum)
		} else if key == "oneWayDelay" {
			f.loadDataCenterDistance(v.(map[string]interface{}))
		} else if key == "delay" {
			f.delay, err = time.ParseDuration(v.(string))
			if err != nil {
				log.Fatalf("delay %v is invalid", v)
			}
		} else if key == "RPCPoolSize" {
			f.poolSize = int(v.(float64))
		} else if key == "keySize" {
			f.keySize = int(v.(float64))
		} else if key == "txnSize" {
			f.txnSize = int(v.(float64))
		} else if key == "openLoop" {
			f.openLoop = v.(bool)
		} else if key == "txnRate" {
			f.txnRate = int(v.(float64))
		} else if key == "totalTxn" {
			f.totalTxn = int(v.(float64))
		} else if key == "duration" {
			f.duration = time.Duration(int64(v.(float64)) * int64(time.Second))
		} else if key == "zipfAlpha" {
			f.zipfAlpha = v.(float64)
		} else if key == "workload" {
			workload := v.(map[string]interface{})
			workloadType := workload["type"].(string)
			if workloadType == "ycsbt" {
				f.workload = YCSBT
			} else if workloadType == "oneTxn" {
				f.workload = ONETXN
			} else if workloadType == "retwis" {
				f.workload = RETWIS
				retwis := workload["retwis"].(map[string]interface{})
				f.addUserRatio = int(retwis["addUserRatio"].(float64))
				f.followUnfollowRatio = int(retwis["followUnfollowRatio"].(float64))
				f.postTweetRatio = int(retwis["postTweetRatio"].(float64))
				f.loadTimelineRatio = int(retwis["loadTimelineRatio"].(float64))
			}
		} else if key == "seed" {
			f.seed = int64(v.(float64))
			if f.seed == 0 {
				rand.Seed(int64(time.Now().Nanosecond()))
			} else {
				rand.Seed(f.seed)
			}
		} else if key == "queueLen" {
			f.queueLen = int(v.(float64))
		} else if key == "retry" {
			retryInfo := v.(map[string]interface{})
			f.retryInterval = time.Duration(int64(retryInfo["interval"].(float64)) * int64(time.Millisecond))
			f.maxRetry = int64(retryInfo["maxRetry"].(float64))
			f.maxSlot = int64(retryInfo["maxSlot"].(float64))
			mode := retryInfo["mode"].(string)
			if mode == "exp" {
				f.retryMode = EXP
			} else if mode == "const" {
				f.retryMode = CONST
			} else if mode == "off" {
				f.retryMode = OFF
			}
		}
	}
}

func (f *FileConfiguration) loadKey() {
	totalPartition := len(f.partitions)
	f.keys = make([][]string, totalPartition)
	var key int64 = 0
	for ; key < f.keyNum; key++ {
		partitionId := key % int64(totalPartition)
		f.keys[partitionId] = append(f.keys[partitionId], utils.ConvertToString(f.keySize, key))
	}
}

func (f *FileConfiguration) GetServerAddressByServerId(serverId string) string {
	if addr, exist := f.servers[serverId]; exist {
		return addr
	} else {
		log.Fatalf("serverId %v does not exist", serverId)
		return ""
	}
}

func (f *FileConfiguration) GetServerIdByPartitionId(partitionId int) string {
	if partitionId >= len(f.partitions) {
		log.Fatalf("partitionId %v does not exist", partitionId)
		return ""
	}
	return f.partitions[partitionId][0]
}

func (f *FileConfiguration) GetServerAddressMap() map[string]string {
	return f.servers
}

func (f *FileConfiguration) GetPartitionIdByServerId(serverId string) int {
	if pId, exist := f.serverToPartitionMap[serverId]; exist {
		return pId
	} else {
		log.Fatalf("serverId %v does not exist", serverId)
		return -1
	}
}

func (f *FileConfiguration) GetServerMode() ServerMode {
	return f.serverMode
}

func (f *FileConfiguration) GetKeyListByPartitionId(partitionId int) []string {
	if partitionId >= len(f.keys) {
		log.Fatalf("partitionId %v does not exist", partitionId)
		return make([]string, 0)
	}

	return f.keys[partitionId]
}

func (f *FileConfiguration) GetPartitionIdByKey(key string) int {
	totalPartition := len(f.partitions)
	i := utils.ConvertToInt(key)
	return int(i) % totalPartition
}

func (f *FileConfiguration) GetDataCenterIdByServerId(serverId string) string {
	if _, exist := f.serverToDataCenterId[serverId]; !exist {
		log.Fatalf("server %v does not exist", serverId)
		return ""
	}
	return f.serverToDataCenterId[serverId]
}

func (f *FileConfiguration) GetDataCenterIdByClientId(clientId int) string {
	if _, exist := f.clientToDataCenterId[clientId]; !exist {
		log.Fatalf("client %v does not exist", clientId)
		return ""
	}
	return f.clientToDataCenterId[clientId]
}

func (f *FileConfiguration) GetMaxDelay(clientDCId string, dcIds []string) time.Duration {
	dis, exist := f.dataCenterDistance[clientDCId]
	if !exist {
		log.Fatalf("client dataCenter id %v does not exist", clientDCId)
		return 0
	}

	var max time.Duration = 0
	for _, dId := range dcIds {
		if d, exist := dis[dId]; exist {
			if d > max {
				max = d
			}
		} else {
			log.Fatalf("dataCenter id %v does not exist", dId)
			return 0
		}
	}
	return max
}

func (f *FileConfiguration) GetServerListByDataCenterId(dataCenterId string) []string {
	if _, exist := f.dataCenterIdToServerIdList[dataCenterId]; !exist {
		log.Fatalf("dataCenter ID %v does not exist", dataCenterId)
		return make([]string, 0)
	}

	return f.dataCenterIdToServerIdList[dataCenterId]
}

func (f *FileConfiguration) GetKeyNum() int64 {
	return f.keyNum
}

func (f *FileConfiguration) GetDelay() time.Duration {
	return f.delay
}

func (f *FileConfiguration) GetConnectionPoolSize() int {
	return f.poolSize
}

func (f *FileConfiguration) GetKeySize() int {
	return f.keySize
}

func (f *FileConfiguration) GetTxnSize() int {
	return f.txnSize
}

func (f *FileConfiguration) GetTxnRate() int {
	return f.txnRate
}

func (f *FileConfiguration) GetExpDuration() time.Duration {
	return f.duration
}

func (f *FileConfiguration) GetOpenLoop() bool {
	return f.openLoop
}

func (f *FileConfiguration) GetZipfAlpha() float64 {
	return f.zipfAlpha
}

func (f *FileConfiguration) GetWorkLoad() WorkLoad {
	return f.workload
}

func (f *FileConfiguration) GetTotalTxn() int {
	return f.totalTxn
}

func (f *FileConfiguration) GetTotalClient() int {
	return f.clients
}

func (f *FileConfiguration) GetRandSeed() int64 {
	return f.seed
}

func (f *FileConfiguration) GetQueueLen() int {
	return f.queueLen
}

func (f *FileConfiguration) GetRetryInterval() time.Duration {
	return f.retryInterval
}

func (f *FileConfiguration) GetRetryMode() RetryMode {
	return f.retryMode
}

func (f *FileConfiguration) GetMaxRetry() int64 {
	return f.maxRetry
}

func (f *FileConfiguration) GetRetryMaxSlot() int64 {
	return f.maxSlot
}

func (f *FileConfiguration) GetAddUserRatio() int {
	return f.addUserRatio
}

func (f *FileConfiguration) GetFollowUnfollowRatio() int {
	return f.followUnfollowRatio
}

func (f *FileConfiguration) GetPostTweetRatio() int {
	return f.postTweetRatio
}

func (f *FileConfiguration) GetLoadTimelineRatio() int {
	return f.loadTimelineRatio
}
