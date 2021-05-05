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
	flag.StringVar(
		&configFile,
		"c",
		"",
		"client configuration file",
	)

	flag.Parse()
	if configFile == "" {
		flag.Usage()
		logrus.Fatal("Invalid configuration file.")
	}
}
