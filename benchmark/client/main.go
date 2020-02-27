package main

import (
	"Carousel-GTS/benchmark/workload"
	"Carousel-GTS/client"
	"Carousel-GTS/configuration"
	"Carousel-GTS/utils"
	"flag"
	"github.com/sirupsen/logrus"
)

var isDebug = false
var clientId = ""
var configFile = ""

func main() {
	parseArgs()
	utils.ConfigLogger(isDebug, clientId)

	c := client.NewClient(clientId, configFile)
	baseWorkload := workload.NewAbstractWorkload(c.Config.GetKeyNum(), c.Config.GetZipfAlpha(), c.Config.GetKeySize())
	var expWorkload workload.Workload
	var exp Experiment
	switch c.Config.GetWorkLoad() {
	case configuration.YCSBT:
		expWorkload = workload.NewYCSBTWorkload(baseWorkload, c.Config.GetTxnSize(), c.Config.GetTxnSize())
	case configuration.ONETXN:
		expWorkload = workload.NewOneTxnWorkload(baseWorkload, c.Config.GetTxnSize(), c.Config.GetTxnSize())
	}

	if c.Config.GetOpenLoop() {
		exp = NewOpenLoopExperiment(c, expWorkload)
	} else {
		exp = NewCloseLoopExperiment(c, expWorkload)
	}

	if exp == nil {
		logrus.Fatalf("experiment is nil")
		return
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
	flag.StringVar(
		&clientId,
		"i",
		"",
		"client id",
	)

	flag.Parse()
	if clientId == "" {
		flag.Usage()
		logrus.Fatal("Invalid client id.")
	}
	if configFile == "" {
		flag.Usage()
		logrus.Fatal("Invalid configuration file.")
	}
}
