package server

import (
	"Carousel-GTS/utils"
	"flag"
	"github.com/sirupsen/logrus"
)

var isDebug bool = false
var serverId string = ""
var configFile string = ""

func main() {
	parseArgs()
	utils.ConfigLogger(isDebug)

	server := NewServer(serverId, configFile)

	server.Start()
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
