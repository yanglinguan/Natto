package configuration

import (
	"Carousel-GTS/utils"
	"encoding/json"
	"io/ioutil"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type ServerMode int

const (
	OCC ServerMode = iota
	PRIORITY
	TwoPL
	TO
	TAPIR
	SPANNER
)

type ConcurrencyControl int

const (
	TWOPL ConcurrencyControl = iota
)

type PriorityMode int

const (
	NOPRIORITY PriorityMode = iota
	PREEMPTION
	POW // Preemption on wait
)

type WorkLoad int

const (
	YCSBT WorkLoad = iota
	ONETXN
	RETWIS
	REORDER
	RANDYCSBT
	SMALLBANK
)

type RetryMode int

const (
	EXP RetryMode = iota
	CONST
	OFF
)

type Configuration interface {
	GetServerAddressByServerId(serverId int) string
	GetLeaderIdByPartitionId(partitionId int) int
	GetServerIdListByPartitionId(partitionId int) []int
	GetServerAddress() []string
	GetPartitionIdByServerId(serverId int) int
	GetRaftPeersByServerId(serverId int) []string
	GetRaftIdByServerId(serverId int) int
	GetRaftPortByServerId(serverId int) string
	GetServerIdByRaftId(raftId int, serverId int) int
	GetTotalPartition() int
	GetExpectPartitionLeaders() []int
	GetFastPath() bool
	GetSuperMajority() int
	GetIsReadOnly() bool
	GetCheckWaiting() bool
	GetTimeWindow() time.Duration
	UsePoissonProcessBetweenArrivals() bool
	GetServerNum() int

	GetServerMode() ServerMode
	GetKeyListByPartitionId(partitionId int) []string
	GetPartitionIdByKey(key string) int
	GetDataCenterIdByServerId(serverId int) int
	GetDataCenterIdByClientId(clientId int) int
	GetMaxDelay(clientDCId int, dcIds map[int]bool) time.Duration
	GetLatencyList(clientDCId int, serverList []int) []int64
	GetServerListByDataCenterId(dataCenterId int) []int
	GetLeaderIdListByDataCenterId(dataCenterId int) []int
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
	GetReplication() bool

	// SmallBank
	GetSbIsHotSpotFixedSize() bool
	GetSbHotSpotFixedSize() int
	GetSbHotSpotPercentage() int
	GetSbHotSpotTxnRatio() int
	GetSbAmalgamateRatio() int
	GetSbBalanceRatio() int
	GetSbDepositCheckingRatio() int
	GetSbSendPaymentRatio() int
	GetSbTransactSavingsRatio() int
	GetSbWriteCheckRatio() int
	GetSbCheckingAccountFlag() string
	GetSbSavingsAccountFlag() string
	GetSbInitBalance() string

	GetSSHIdentity() string
	GetSSHUsername() string
	GetRunDir() string

	GetHighPriorityRate() int
	GetTargetRate() int

	IsDynamicLatency() bool
	GetProbeWindowMinSize() int
	GetProbeWindowLen() time.Duration
	IsProbeBlocking() bool
	GetProbeInterval() time.Duration
	IsProbeTime() bool

	IsEarlyAbort() bool
	IsConditionalPrepare() bool

	IsOptimisticReorder() bool
	UseNetworkTimestamp() bool

	HighTxnOnly() bool
	QueuePos() int
	UsePriorityScheduler() bool
	ReadBeforeCommitReplicate() bool
	ForwardReadToCoord() bool

	Popular() int64

	GetSinglePartitionRate() int

	GetNetworkMeasureAddr(dcId int) string
	GetDCNum() int
	GetPredictDelayPercentile() int
	GetUpdateInterval() time.Duration

	GetPartitionInfo() [][]int
	IsCoordServer(serverId int) bool

	GetPriorityMode() PriorityMode
	IsClientPriority() bool

	GetRetwisHighPriorityTxn() string
	GetSbHighPriorityTxn() string
}

type FileConfiguration struct {
	// serverId -> server Address (ip:port)
	servers                    []string
	dcNum                      int
	clients                    int
	dataPartitions             int
	partitions                 [][]int
	raftPeers                  [][]string
	raftToServerId             [][]int
	expectPartitionLeaders     []int
	dataCenterIdToLeaderIdList [][]int

	serverToRaftId   []int
	serverToRaftPort []string

	serverToPartitionId []int

	serverMode ServerMode

	keys   [][]string
	keyNum int64

	// dataCenterId -> (dataCenterId -> distance)
	dataCenterDistance [][]time.Duration
	// serverId -> dataCenterId
	serverToDataCenterId []int
	// dataCenterId -> serverId
	dataCenterIdToServerIdList [][]int

	// clientId -> dataCenterId
	clientToDataCenterId []int

	// dcId -> network measurement machine addr
	dcIdToNetworkMeasurementMachineAddr []string

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

	addUserRatio          int
	followUnfollowRatio   int
	postTweetRatio        int
	loadTimelineRatio     int
	retwisHighPriorityTxn string

	// SmallBank
	sbIsHotSpotFixedSize   bool
	sbHotSpotFixedSize     int
	sbHotSpotPercentage    int
	sbHotSpotTxnRatio      int
	sbAmalgamateRatio      int
	sbBalanceRatio         int
	sbDepositCheckRatio    int
	sbSendPaymentRatio     int
	sbTransactSavingsRatio int
	sbWriteCheckRatio      int
	sbCheckingFlag         string
	sbSavingsFlag          string
	sbInitBalance          string // encoded float64 instead of a string of a number
	sbHighPriorityTxn      string

	isReplication     bool
	replicationFactor int
	isFastPath        bool
	failure           float64
	superMajority     int

	isReadOnly bool

	username string
	identity string
	runDir   string

	checkWaiting bool // for gts-graph, check the waiting txn there is write-read conflict

	highPriorityRate   int
	targetRate         int // client sends at this target rate
	timeWindow         time.Duration
	conditionalPrepare bool

	dynamicLatency     bool
	probeWindowLen     time.Duration
	probeWindowMinSize int
	probeInterval      time.Duration
	probeBlocking      bool
	probeTime          bool

	optimisticReorder bool
	networkTimestamp  bool
	poissonProcess    bool

	fastCommit bool

	highTxnOnly bool

	queuePos                  int
	priorityScheduler         bool
	readBeforeCommitReplicate bool
	forwardReadToCoord        bool
	popular                   int64

	randycsbtSingle        int
	predictDelayPercentile int
	updateInterval         time.Duration

	cc             ConcurrencyControl
	priorityMode   PriorityMode
	clientPriority bool
}

func NewFileConfiguration(filePath string) *FileConfiguration {
	c := &FileConfiguration{}
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
	//f.loadPartitions(config["partitions"].([]interface{}))
	f.loadExperiment(config["experiment"].(map[string]interface{}))

	f.loadKey()
}

func (f *FileConfiguration) loadServers(config map[string]interface{}) {
	partitionNum := int(config["partitions"].(float64))
	serverNum := int(config["nums"].(float64))
	f.dataPartitions = int(config["dataPartitions"].(float64))

	f.replicationFactor = int(config["replicationFactor"].(float64))
	//f.isReplication = f.replicationFactor != 1
	f.failure = config["failure"].(float64)
	f.superMajority = int(math.Ceil(f.failure*1.5) + 1)
	f.dcNum = int(config["dcNum"].(float64))
	if serverNum%f.replicationFactor != 0 {
		log.Fatalf("the number of server %v and replication factor %v does not much", serverNum, f.replicationFactor)
		return
	}
	dataServerNum := f.dataPartitions * f.replicationFactor
	machines := config["machines"].([]interface{})
	dataServerMachineNum := len(machines)
	coordMachines := config["coordMachines"].([]interface{})
	coordMachineNum := len(coordMachines)
	rpcPortBase := int(config["rpcPortBase"].(float64))
	raftPortBase := int(config["raftPortBase"].(float64))

	f.dataCenterIdToServerIdList = make([][]int, f.dcNum)
	f.dataCenterIdToLeaderIdList = make([][]int, f.dcNum)
	f.servers = make([]string, serverNum)
	f.serverToPartitionId = make([]int, serverNum)
	f.serverToRaftId = make([]int, serverNum)
	f.serverToRaftPort = make([]string, serverNum)
	f.serverToDataCenterId = make([]int, serverNum)
	f.partitions = make([][]int, partitionNum)
	f.raftPeers = make([][]string, partitionNum)
	f.raftToServerId = make([][]int, partitionNum)
	f.expectPartitionLeaders = make([]int, partitionNum)

	for sId := 0; sId < serverNum; sId++ {
		pId := sId / f.replicationFactor
		mId := sId % dataServerMachineNum
		ip := machines[mId].(string)
		if sId >= dataServerNum {
			mId = (sId - dataServerNum) % coordMachineNum
			ip = coordMachines[mId].(string)
		}

		dcId := sId % f.dcNum
		rpcPort := strconv.Itoa(rpcPortBase + sId)
		raftPort := strconv.Itoa(raftPortBase + sId)

		rpcAddr := ip + ":" + rpcPort
		raftAddr := "http://" + ip + ":" + raftPort

		f.servers[sId] = rpcAddr
		f.partitions[pId] = append(f.partitions[pId], sId)
		f.serverToRaftId[sId] = len(f.raftPeers[pId])
		f.raftToServerId[pId] = append(f.raftToServerId[pId], sId)
		f.raftPeers[pId] = append(f.raftPeers[pId], raftAddr)
		f.serverToDataCenterId[sId] = dcId
		f.serverToPartitionId[sId] = pId
		f.serverToRaftPort[sId] = raftPort

		if f.dcNum == 3 {
			leaderMap := make(map[int]bool)
			var leaderList []int
			if partitionNum == 9 {
				leaderList = []int{0, 3, 6, 10, 13, 16, 20, 23, 26}
			} else if partitionNum == 6 {
				leaderList = []int{0, 3, 7, 10, 14, 17}
			} else if partitionNum == 3 {
				leaderList = []int{0, 4, 8}
			} else if partitionNum == 12 {
				leaderList = []int{0, 13, 26, 3, 16, 29, 6, 19, 32, 9, 22, 35}
			}
			for _, l := range leaderList {
				leaderMap[l] = true
			}
			if leaderMap[sId] {
				f.expectPartitionLeaders[pId] = sId
				f.dataCenterIdToLeaderIdList[dcId] = append(f.dataCenterIdToLeaderIdList[dcId], sId)
			}
		} else {
			if sId%f.replicationFactor == 0 {
				f.expectPartitionLeaders[pId] = sId
				f.dataCenterIdToLeaderIdList[dcId] = append(f.dataCenterIdToLeaderIdList[dcId], sId)
			}
		}

		f.dataCenterIdToServerIdList[dcId] = append(f.dataCenterIdToServerIdList[dcId], sId)

		log.Infof("Server %v: addr %v, raftAddr %v, raftGroup %v, partitionId %v, dataCenterId %v, isLeader %v",
			sId, rpcAddr, raftAddr, pId, pId, dcId, sId%f.replicationFactor == 0)
	}
	log.Debugf("datacenter leader %v", f.dataCenterIdToLeaderIdList)
}

func (f *FileConfiguration) loadClients(config map[string]interface{}) {
	f.clients = int(config["nums"].(float64))
	machinesNum := len(config["machines"].([]interface{}))
	f.clientToDataCenterId = make([]int, f.clients)
	for id := 0; id < f.clients; id++ {
		idx := id % machinesNum
		dcId := idx % f.dcNum
		f.clientToDataCenterId[id] = dcId
	}
	networkMeasureMachines := config["networkMeasureMachines"].([]interface{})
	if len(networkMeasureMachines) > 0 {
		f.dcIdToNetworkMeasurementMachineAddr = make([]string, f.dcNum)
		portBase := int(config["networkMeasurePortBase"].(float64))
		if len(networkMeasureMachines) != f.dcNum {
			log.Fatalf("there should be one machine per dc to measure network delay")
		}
		for dcId, ip := range networkMeasureMachines {
			port := strconv.Itoa(portBase + dcId)
			f.dcIdToNetworkMeasurementMachineAddr[dcId] = ip.(string) + ":" + port
		}
	}
}

func (f *FileConfiguration) loadDataCenterDistance(config []interface{}) {
	var err error
	if f.dcNum != len(config) {
		log.Fatalf("total dataCenter required %v provided %v", f.dcNum, len(config))
		return
	}
	f.dataCenterDistance = make([][]time.Duration, f.dcNum)
	for dcId, disList := range config {
		f.dataCenterDistance[dcId] = make([]time.Duration, f.dcNum)
		for dstId, dis := range disList.([]interface{}) {
			f.dataCenterDistance[dcId][dstId], err = time.ParseDuration(dis.(string))
			if err != nil {
				log.Fatalf("Sets delay (%v, %v) fails: %v", dcId, dstId, err)
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
			} else if mode == "priority" {
				f.serverMode = PRIORITY
			} else if mode == "2PL" {
				f.serverMode = TwoPL
			} else if mode == "TO" {
				f.serverMode = TO
			} else if mode == "tapir" {
				f.serverMode = TAPIR
			} else if mode == "spanner" {
				f.serverMode = SPANNER
			} else {
				log.Fatalf("server mode should be one of occ, priority, 2PL, TO")
			}
		} else if key == "totalKey" {
			keyNum := v.(float64)
			f.keyNum = int64(keyNum)
		} else if key == "latency" {
			items := v.(map[string]interface{})
			f.loadDataCenterDistance(items["oneWayDelay"].([]interface{}))
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
			f.duration, err = time.ParseDuration(v.(string))
			if err != nil {
				log.Fatalf("duration %v is invalid, error %v", v, err)
			}
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
				f.retwisHighPriorityTxn = retwis["highPriorityTxn"].(string)
			} else if workloadType == "smallbank" {
				f.workload = SMALLBANK
				sb := workload["smallbank"].(map[string]interface{})
				f.sbIsHotSpotFixedSize = sb["isHotSpotFixedSize"].(bool)
				f.sbHotSpotFixedSize = int(sb["hotSpotFixedSize"].(float64))
				f.sbHotSpotPercentage = int(sb["hotSpotPercentage"].(float64))
				f.sbHotSpotTxnRatio = int(sb["hotSpotTxnRatio"].(float64))
				f.sbAmalgamateRatio = int(sb["amalgamateRatio"].(float64))
				f.sbBalanceRatio = int(sb["balance"].(float64))
				f.sbDepositCheckRatio = int(sb["depositChecking"].(float64))
				f.sbSendPaymentRatio = int(sb["sendPayment"].(float64))
				f.sbTransactSavingsRatio = int(sb["transactSavings"].(float64))
				f.sbWriteCheckRatio = int(sb["writeCheck"].(float64))
				f.sbCheckingFlag = sb["checkingFlag"].(string)
				f.sbSavingsFlag = sb["savingsFlag"].(string)
				f.sbInitBalance = utils.ConvertFloatToString(sb["initBalance"].(float64))
				f.sbHighPriorityTxn = sb["highPriorityTxn"].(string)
			} else if workloadType == "reorder" {
				f.workload = REORDER
			} else if workloadType == "randYcsbt" {
				f.workload = RANDYCSBT
				randycsbt := workload["randYcsbt"].(map[string]interface{})
				f.randycsbtSingle = int(randycsbt["single"].(float64))
			}
			f.highPriorityRate = int(workload["highPriority"].(float64))
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
			f.retryInterval, err = time.ParseDuration(retryInfo["interval"].(string))
			if err != nil {
				log.Fatalf("retry interval %v is invalid, error %v", v, err)
			}

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
		} else if key == "ssh" {
			items := v.(map[string]interface{})
			f.username = items["username"].(string)
			f.identity = items["identity"].(string)
		} else if key == "runDir" {
			f.runDir = v.(string)
		} else if key == "fastPath" {
			f.isFastPath = v.(bool)
		} else if key == "readOnly" {
			f.isReadOnly = v.(bool)
		} else if key == "checkWaiting" {
			f.checkWaiting = v.(bool)
		} else if key == "replication" {
			f.isReplication = v.(bool)
		} else if key == "targetRate" {
			f.targetRate = int(v.(float64))
		} else if key == "timeWindow" {
			f.timeWindow, err = time.ParseDuration(v.(string))
			if err != nil {
				log.Fatalf("timeWindow %v is invalid, error %v", v, err)
			}
			if f.timeWindow < 0 {
				f.timeWindow = time.Duration(math.MaxInt64)
			}
		} else if key == "dynamicLatency" {
			items := v.(map[string]interface{})
			f.dynamicLatency = items["mode"].(bool)
			f.probeWindowLen, err = time.ParseDuration(items["probeWindowLen"].(string))
			if err != nil {
				log.Fatalf("probeWindowLen %v is invalid", v)
			}
			f.probeWindowMinSize = int(items["probeWindowMinSize"].(float64))
			f.probeInterval, err = time.ParseDuration(items["probeInterval"].(string))
			if err != nil {
				log.Fatalf("probeInterval %v is invalid error %v", v, err)
			}
			f.probeBlocking = items["blocking"].(bool)
			f.probeTime = items["probeTime"].(bool)
			f.predictDelayPercentile = int(items["percentile"].(float64))
			f.updateInterval, err = time.ParseDuration(items["updateInterval"].(string))
			if err != nil {
				log.Fatalf("updateInterval %v is invalid", v)
			}
		} else if key == "conditionalPrepare" {
			f.conditionalPrepare = v.(bool)
		} else if key == "optimisticReorder" {
			f.optimisticReorder = v.(bool)
		} else if key == "networkTimestamp" {
			f.networkTimestamp = v.(bool)
		} else if key == "poissonProcess" {
			// poisson process between arrivals
			f.poissonProcess = v.(bool)
		} else if key == "fastCommit" {
			f.fastCommit = v.(bool)
		} else if key == "highTxnOnly" {
			f.highTxnOnly = v.(bool)
		} else if key == "queuePos" {
			f.queuePos = int(v.(float64))
		} else if key == "priorityScheduler" {
			f.priorityScheduler = v.(bool)
		} else if key == "readBeforeCommitReplicate" {
			f.readBeforeCommitReplicate = v.(bool)
		} else if key == "forwardReadToCoord" {
			f.forwardReadToCoord = v.(bool)
		} else if key == "popular" {
			f.popular = int64(v.(float64))
		} else if key == "concurrencyControl" {
			cc := v.(string)
			if cc == "2PL" {
				f.cc = TWOPL
			}
		} else if key == "priorityMode" {
			pm := v.(string)
			if pm == "preemption" {
				f.priorityMode = PREEMPTION
			} else if pm == "pow" {
				f.priorityMode = POW
			} else {
				f.priorityMode = NOPRIORITY
			}
		} else if key == "clientPriority" {
			f.clientPriority = v.(bool)
		}
	}
}

func (f *FileConfiguration) GetPriorityMode() PriorityMode {
	return f.priorityMode
}

func (f *FileConfiguration) loadKey() {
	totalPartition := len(f.partitions)
	f.keys = make([][]string, totalPartition)
	var key int64 = 0
	for ; key < f.keyNum; key++ {
		partitionId := key % int64(f.dataPartitions)
		f.keys[partitionId] = append(f.keys[partitionId], utils.ConvertToString(f.keySize, key))
	}
}

func (f *FileConfiguration) GetServerAddressByServerId(serverId int) string {
	if serverId > len(f.servers) {
		log.Fatalf("serverId %v does not exist", serverId)
		return ""
	}
	return f.servers[serverId]
}

func (f *FileConfiguration) GetLeaderIdByPartitionId(partitionId int) int {
	if partitionId >= len(f.partitions) {
		log.Fatalf("partitionId %v does not exist", partitionId)
		return -1
	}
	log.Debugf("partition %v leader is %v", partitionId, f.expectPartitionLeaders[partitionId])
	return f.expectPartitionLeaders[partitionId]
}

func (f *FileConfiguration) GetServerIdListByPartitionId(partitionId int) []int {
	if partitionId >= len(f.partitions) {
		log.Fatalf("partitionId %v does not exist", partitionId)
		return nil
	}
	if f.isFastPath {
		return f.partitions[partitionId]
	} else {
		return []int{f.GetLeaderIdByPartitionId(partitionId)}
	}
}

func (f *FileConfiguration) GetServerNum() int {
	return len(f.servers)
}

func (f *FileConfiguration) GetServerAddress() []string {
	return f.servers
}

func (f *FileConfiguration) GetPartitionIdByServerId(serverId int) int {
	if serverId > len(f.serverToPartitionId) {
		log.Fatalf("serverId %v does not exist", serverId)
		return -1
	} else {
		return f.serverToPartitionId[serverId]
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

func (f *FileConfiguration) GetPartitionInfo() [][]int {
	return f.partitions
}

func (f *FileConfiguration) GetPartitionIdByKey(key string) int {
	totalPartition := f.dataPartitions
	if f.GetWorkLoad() == SMALLBANK {
		n := 0
		if strings.HasSuffix(key, f.GetSbCheckingAccountFlag()) {
			n = len(f.GetSbCheckingAccountFlag())
		} else if strings.HasSuffix(key, f.GetSbSavingsAccountFlag()) {
			n = len(f.GetSbSavingsAccountFlag())
		} else {
			log.Fatalf("Invalid suffix for key %s in SmallBank Workload", key)
			return -1
		}
		k := key[:len(key)-n]
		i := utils.ConvertToInt(k)
		return int(i) % totalPartition
	} else {
		i := utils.ConvertToInt(key)
		return int(i) % totalPartition
	}
}

func (f *FileConfiguration) GetDataCenterIdByServerId(serverId int) int {
	if serverId >= len(f.serverToDataCenterId) {
		log.Fatalf("server %v does not exist", serverId)
		return -1
	}
	return f.serverToDataCenterId[serverId]
}

func (f *FileConfiguration) GetDataCenterIdByClientId(clientId int) int {
	if clientId >= len(f.clientToDataCenterId) {
		log.Fatalf("client %v does not exist", clientId)
		return -1
	}

	return f.clientToDataCenterId[clientId]
}

func (f *FileConfiguration) GetMaxDelay(clientDCId int, dcIds map[int]bool) time.Duration {
	if clientDCId >= f.dcNum || clientDCId < 0 {
		log.Fatalf("invalid dataCenter Id %v should < %v", clientDCId, f.dcNum)
	}

	dis := f.dataCenterDistance[clientDCId]

	var max time.Duration = 0
	for dId := range dcIds {
		if dId > f.dcNum || dId < 0 {
			log.Fatalf("invalid dataCenter Id %v should < %v", clientDCId, f.dcNum)
		}
		d := dis[dId]
		if d > max {
			max = d
		}
	}
	return max
}

func (f *FileConfiguration) GetLatencyList(clientDCId int, serverList []int) []int64 {
	if clientDCId >= f.dcNum || clientDCId < 0 {
		log.Fatalf("invalid dataCenter Id %v should < %v", clientDCId, f.dcNum)
	}
	dis := f.dataCenterDistance[clientDCId]
	result := make([]int64, len(serverList))
	for i, sId := range serverList {
		dcId := f.GetDataCenterIdByServerId(sId)
		result[i] = dis[dcId].Nanoseconds()
	}
	return result
}

func (f *FileConfiguration) GetServerListByDataCenterId(dataCenterId int) []int {
	if dataCenterId >= f.dcNum || dataCenterId < 0 {
		log.Fatalf("invalid dataCenter Id %v should < %v", dataCenterId, f.dcNum)
		return nil
	}

	return f.dataCenterIdToServerIdList[dataCenterId]
}

func (f *FileConfiguration) GetLeaderIdListByDataCenterId(dataCenterId int) []int {
	if dataCenterId >= f.dcNum || dataCenterId < 0 {
		log.Fatalf("invalid dataCenter Id %v should < %v", dataCenterId, f.dcNum)
		return nil
	}

	return f.dataCenterIdToLeaderIdList[dataCenterId]
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

func (f *FileConfiguration) GetSbIsHotSpotFixedSize() bool {
	return f.sbIsHotSpotFixedSize
}

func (f *FileConfiguration) GetSbHotSpotFixedSize() int {
	return f.sbHotSpotFixedSize
}

func (f *FileConfiguration) GetSbHotSpotPercentage() int {
	return f.sbHotSpotPercentage
}

func (f *FileConfiguration) GetSbHotSpotTxnRatio() int {
	return f.sbHotSpotTxnRatio
}

func (f *FileConfiguration) GetSbAmalgamateRatio() int {
	return f.sbAmalgamateRatio
}

func (f *FileConfiguration) GetSbBalanceRatio() int {
	return f.sbBalanceRatio
}

func (f *FileConfiguration) GetSbDepositCheckingRatio() int {
	return f.sbDepositCheckRatio
}

func (f *FileConfiguration) GetSbSendPaymentRatio() int {
	return f.sbSendPaymentRatio
}

func (f *FileConfiguration) GetSbTransactSavingsRatio() int {
	return f.sbTransactSavingsRatio
}

func (f *FileConfiguration) GetSbWriteCheckRatio() int {
	return f.sbWriteCheckRatio
}

func (f *FileConfiguration) GetSbCheckingAccountFlag() string {
	return f.sbCheckingFlag
}

func (f *FileConfiguration) GetSbSavingsAccountFlag() string {
	return f.sbSavingsFlag
}

func (f *FileConfiguration) GetSbInitBalance() string {
	return f.sbInitBalance
}

func (f *FileConfiguration) GetSinglePartitionRate() int {
	return f.randycsbtSingle
}

func (f *FileConfiguration) GetRaftPeersByServerId(serverId int) []string {
	if serverId >= len(f.servers) || serverId < 0 {
		log.Fatalf("server %d does not exist", serverId)
		return make([]string, 0)
	}
	raftGroupId := serverId / f.replicationFactor
	log.Debugf("server %v raft peer is %v raftgroup is %v", serverId, f.raftPeers[raftGroupId], raftGroupId)
	return f.raftPeers[raftGroupId]
}

func (f *FileConfiguration) GetRaftIdByServerId(serverId int) int {
	if serverId >= len(f.serverToRaftId) || serverId < 0 {
		log.Fatalf("server %d does not exist", serverId)
		return -1
	}
	return f.serverToRaftId[serverId]
}

func (f *FileConfiguration) GetRaftPortByServerId(serverId int) string {
	if serverId >= len(f.serverToRaftId) || serverId < 0 {
		log.Fatalf("server %d does not exist", serverId)
		return ""
	}

	return f.serverToRaftPort[serverId]
}

func (f *FileConfiguration) GetServerIdByRaftId(raftId int, serverId int) int {
	if serverId >= len(f.servers) || serverId < 0 {
		log.Fatalf("server %d does not exist", serverId)
		return -1
	}

	raftGroupId := serverId / f.replicationFactor
	sId := f.raftToServerId[raftGroupId][raftId]
	return sId
}

func (f *FileConfiguration) GetReplication() bool {
	return f.isReplication
}

func (f *FileConfiguration) GetTotalPartition() int {
	return f.dataPartitions
}

func (f *FileConfiguration) GetExpectPartitionLeaders() []int {
	return f.expectPartitionLeaders
}

func (f *FileConfiguration) GetSSHIdentity() string {
	return f.identity
}

func (f *FileConfiguration) GetSSHUsername() string {
	return f.username
}

func (f *FileConfiguration) GetRunDir() string {
	return f.runDir
}

func (f *FileConfiguration) GetFastPath() bool {
	return f.isFastPath
}

func (f *FileConfiguration) GetSuperMajority() int {
	return f.superMajority
}

func (f *FileConfiguration) GetIsReadOnly() bool {
	return f.isReadOnly
}

func (f *FileConfiguration) GetCheckWaiting() bool {
	return f.checkWaiting
}

func (f *FileConfiguration) GetHighPriorityRate() int {
	return f.highPriorityRate
}

func (f *FileConfiguration) GetTargetRate() int {
	return f.targetRate
}

func (f *FileConfiguration) GetTimeWindow() time.Duration {
	return f.timeWindow
}

func (f *FileConfiguration) IsDynamicLatency() bool {
	return f.dynamicLatency
}

func (f *FileConfiguration) GetProbeWindowMinSize() int {
	return f.probeWindowMinSize
}

func (f *FileConfiguration) GetProbeWindowLen() time.Duration {
	return f.probeWindowLen
}

func (f *FileConfiguration) GetProbeInterval() time.Duration {
	return f.probeInterval
}

func (f *FileConfiguration) IsProbeBlocking() bool {
	return f.probeBlocking
}

func (f *FileConfiguration) IsProbeTime() bool {
	return f.probeTime
}

func (f *FileConfiguration) IsEarlyAbort() bool {
	return f.timeWindow != 0
}

func (f *FileConfiguration) IsConditionalPrepare() bool {
	return f.conditionalPrepare
}

func (f *FileConfiguration) IsOptimisticReorder() bool {
	return f.optimisticReorder
}

func (f *FileConfiguration) UseNetworkTimestamp() bool {
	return f.networkTimestamp
}

func (f *FileConfiguration) UsePoissonProcessBetweenArrivals() bool {
	return f.poissonProcess
}

func (f *FileConfiguration) HighTxnOnly() bool {
	return f.highTxnOnly
}

func (f *FileConfiguration) QueuePos() int {
	return f.queuePos
}

func (f *FileConfiguration) UsePriorityScheduler() bool {
	return f.priorityScheduler
}

func (f *FileConfiguration) ReadBeforeCommitReplicate() bool {
	return f.readBeforeCommitReplicate
}

func (f *FileConfiguration) ForwardReadToCoord() bool {
	return f.forwardReadToCoord
}

func (f *FileConfiguration) Popular() int64 {
	return f.popular
}

func (f *FileConfiguration) GetNetworkMeasureAddr(dcId int) string {
	return f.dcIdToNetworkMeasurementMachineAddr[dcId]
}

func (f *FileConfiguration) GetDCNum() int {
	return f.dcNum
}

func (f *FileConfiguration) GetPredictDelayPercentile() int {
	return f.predictDelayPercentile
}

func (f *FileConfiguration) GetUpdateInterval() time.Duration {
	return f.updateInterval
}

func (f *FileConfiguration) IsCoordServer(serverId int) bool {
	return serverId >= f.dataPartitions*f.replicationFactor
}

func (f *FileConfiguration) IsClientPriority() bool {
	return f.clientPriority
}

func (f *FileConfiguration) GetRetwisHighPriorityTxn() string {
	return f.retwisHighPriorityTxn
}

func (f *FileConfiguration) GetSbHighPriorityTxn() string {
	return f.sbHighPriorityTxn
}
