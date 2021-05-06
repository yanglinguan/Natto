package main

import (
	"Carousel-GTS/utils"
	"flag"
	"github.com/sirupsen/logrus"
)

var isDebug = false
var dcId = -1
var configFile = ""

func main() {
	parseArgs()
	utils.ConfigLogger(isDebug)

	c := NewNetworkMeasure(dcId, configFile)

	c.Start()
}

func parseArgs() {
	flag.BoolVar(
		&isDebug,
		"d",
		false,
		"debug mode",
	)
	flag.IntVar(
		&dcId,
		"i",
		-1,
		"datacenter Id",
	)
	flag.StringVar(
		&configFile,
		"c",
		"",
		"client configuration file",
	)

	flag.Parse()
	if dcId == -1 {
		flag.Usage()
		logrus.Fatalf("Invalid dcId")
	}
	if configFile == "" {
		flag.Usage()
		logrus.Fatal("Invalid configuration file.")
	}
}
