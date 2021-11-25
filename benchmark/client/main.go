package main

import (
	"Carousel-GTS/benchmark/workload"
	"Carousel-GTS/client"
	"Carousel-GTS/configuration"
	"Carousel-GTS/utils"
	"flag"
	"time"

	"github.com/sirupsen/logrus"
)

var isDebug = false
var clientId = -1
var configFile = ""
var startTime int64

func main() {
	parseArgs()
	utils.ConfigLogger(isDebug)

	config := configuration.NewFileConfiguration(configFile)
	var c client.IFClient
	switch config.GetServerMode() {
	case configuration.TAPIR:
		c = client.NewTapirClient(clientId, config)
	case configuration.SPANNER:
		c = client.NewSpannerClient(clientId, config)
	default:
		c = client.NewClient(clientId, configFile)
	}

	baseWorkload := workload.NewAbstractWorkload(
		config.GetKeyNum(),
		config.GetZipfAlpha(),
		config.GetKeySize(),
		config.GetHighPriorityRate(),
		config.GetTotalPartition(),
		config.IsClientPriority(),
	)
	var expWorkload workload.Workload
	var exp Experiment
	switch config.GetWorkLoad() {
	case configuration.YCSBT:
		expWorkload = workload.NewYCSBTWorkload(
			baseWorkload, config.GetTxnSize(), config.GetTxnSize())
		break
	case configuration.ONETXN:
		expWorkload = workload.NewOneTxnWorkload(
			baseWorkload, config.GetTxnSize(), config.GetTxnSize())
		break
	case configuration.RETWIS:
		expWorkload = workload.NewRetwisWorkload(
			baseWorkload,
			config.GetAddUserRatio(),
			config.GetFollowUnfollowRatio(),
			config.GetPostTweetRatio(),
			config.GetLoadTimelineRatio())
		break
	case configuration.SMALLBANK:
		expWorkload = workload.NewSmallBankWorkload(
			baseWorkload,
			config.GetSbIsHotSpotFixedSize(),
			config.GetSbHotSpotFixedSize(),
			config.GetSbHotSpotPercentage(),
			config.GetSbHotSpotTxnRatio(),
			config.GetSbAmalgamateRatio(),
			config.GetSbBalanceRatio(),
			config.GetSbDepositCheckingRatio(),
			config.GetSbSendPaymentRatio(),
			config.GetSbTransactSavingsRatio(),
			config.GetSbWriteCheckRatio(),
			config.GetSbCheckingAccountFlag(),
			config.GetSbSavingsAccountFlag(),
		)
		break
	case configuration.REORDER:
		clientDCId := config.GetDataCenterIdByClientId(clientId)
		leaderIdList := config.GetLeaderIdListByDataCenterId(clientDCId)
		// assume there is only one leader in a DC
		localPartition := config.GetPartitionIdByServerId(leaderIdList[0])
		expWorkload = workload.NewReorderWorkload(
			baseWorkload, config.GetTotalPartition(), localPartition)
		break
	case configuration.RANDYCSBT:
		expWorkload = workload.NewRandSizeYcsbWorkload(
			baseWorkload, config.GetTxnSize(), config.GetTxnSize(), config.GetSinglePartitionRate())
		break
	default:
		logrus.Fatalf("workload should be: ycsbt, oneTxn, retwis, smallbank, or reorder")
		return
	}

	if config.GetOpenLoop() {
		exp = NewOpenLoopExperiment(c, config, expWorkload, config.HighTxnOnly())
	} else {
		exp = NewCloseLoopExperiment(c, config, expWorkload)
	}

	if exp == nil {
		logrus.Fatalf("experiment is nil")
		return
	}

	// all client start around the same time
	c.Start()
	d := time.Duration(startTime - time.Now().UnixNano())
	if d > 0 {
		logrus.Warnf("client wait %v to start", d)
		time.Sleep(d)
	}
	logrus.Warnf("client %v start", clientId)
	exp.Execute()

	c.Close()
}

func parseArgs() {
	flag.BoolVar(
		&isDebug,
		"d",
		false,
		"debug mode",
	)
	flag.StringVar(
		&configFile,
		"c",
		"",
		"client configuration file",
	)
	flag.IntVar(
		&clientId,
		"i",
		-1,
		"client id",
	)
	flag.Int64Var(
		&startTime,
		"t",
		0,
		"start Time",
	)

	flag.Parse()
	if clientId == -1 {
		flag.Usage()
		logrus.Fatal("Invalid client id.")
	}
	if configFile == "" {
		flag.Usage()
		logrus.Fatal("Invalid configuration file.")
	}
}
