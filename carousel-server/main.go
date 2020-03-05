package main

import (
	"Carousel-GTS/server"
	"Carousel-GTS/utils"
	"flag"
	"github.com/sirupsen/logrus"
)

var isDebug = false
var serverId = ""
var configFile = ""

func main() {
	parseArgs()
	utils.ConfigLogger(isDebug)

	s := server.NewServer(serverId, configFile)

	s.Start()
}

func parseArgs() {
	flag.BoolVar(
		&isDebug,
		"d",
		false,
		"debug mode",
	)

	flag.StringVar(
		&serverId,
		"i",
		"",
		"server id",
	)

	flag.StringVar(
		&configFile,
		"c",
		"",
		"server configuration file",
	)

	flag.Parse()

	if serverId == "" {
		flag.Usage()
		logrus.Fatal("Invalid server id.")
	}
	if configFile == "" {
		flag.Usage()
		logrus.Fatal("Invalid configuration file.")
	}

}
