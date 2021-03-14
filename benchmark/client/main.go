package main

import (
	"Carousel-GTS/benchmark/workload"
	"Carousel-GTS/client"
	"Carousel-GTS/configuration"
	"Carousel-GTS/utils"
	"flag"
	"github.com/sirupsen/logrus"
	"time"
)

var isDebug = false
var clientId = -1
var configFile = ""
var startTime int64

func main() {
	parseArgs()
	utils.ConfigLogger(isDebug)

	c := client.NewClient(clientId, configFile)
	baseWorkload := workload.NewAbstractWorkload(
		c.Config.GetKeyNum(),
		c.Config.GetZipfAlpha(),
		c.Config.GetKeySize(),
		c.Config.GetHighPriorityRate(),
	)
	var expWorkload workload.Workload
	var exp Experiment
	switch c.Config.GetWorkLoad() {
	case configuration.YCSBT:
		expWorkload = workload.NewYCSBTWorkload(
			baseWorkload, c.Config.GetTxnSize(), c.Config.GetTxnSize())
		break
	case configuration.ONETXN:
		expWorkload = workload.NewOneTxnWorkload(
			baseWorkload, c.Config.GetTxnSize(), c.Config.GetTxnSize())
		break
	case configuration.RETWIS:
		expWorkload = workload.NewRetwisWorkload(
			baseWorkload,
			c.Config.GetAddUserRatio(),
			c.Config.GetFollowUnfollowRatio(),
			c.Config.GetPostTweetRatio(),
			c.Config.GetLoadTimelineRatio())
		break
	case configuration.REORDER:
		clientDCId := c.Config.GetDataCenterIdByClientId(clientId)
		leaderIdList := c.Config.GetLeaderIdListByDataCenterId(clientDCId)
		// assume there is only one leader in a DC
		localPartition := c.Config.GetPartitionIdByServerId(leaderIdList[0])
		expWorkload = workload.NewReorderWorkload(
			baseWorkload, c.Config.GetTotalPartition(), localPartition)
		break
	case configuration.RANDYCSBT:
		expWorkload = workload.NewRandSizeYcsbWorkload(
			baseWorkload, c.Config.GetTxnSize(), c.Config.GetTxnSize())
		break
	default:
		logrus.Fatalf("workload should be: ycsbt, oneTxn, retwis or reorder")
		return
	}

	if c.Config.GetOpenLoop() {
		exp = NewOpenLoopExperiment(c, expWorkload, c.Config.HighTxnOnly())
	} else {
		exp = NewCloseLoopExperiment(c, expWorkload)
	}

	if exp == nil {
		logrus.Fatalf("experiment is nil")
		return
	}

	// all client start around the same time
	d := time.Duration(startTime - time.Now().UnixNano())
	if d > 0 {
		logrus.Warnf("client wait %v to start", d)
		time.Sleep(d)
	}

	exp.Execute()

	c.PrintTxnStatisticData()
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
