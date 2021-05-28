package main

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/server"
	"Carousel-GTS/tapir"
	"Carousel-GTS/utils"
	"flag"
	"github.com/sirupsen/logrus"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
)

var isDebug = false
var serverId = -1
var configFile = ""
var cpuProfile = ""

func main() {
	parseArgs()
	utils.ConfigLogger(isDebug)

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			logrus.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			logrus.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	config := configuration.NewFileConfiguration(configFile)
	if config.GetServerMode() == configuration.TAPIR {
		s := tapir.NewServer(strconv.Itoa(serverId), config.GetQueueLen(), false, 0)
		addr := config.GetServerAddressByServerId(serverId)
		port := strings.Split(addr, ":")[1]
		pId := config.GetPartitionIdByServerId(serverId)
		s.InitData(
			config.GetKeyListByPartitionId(pId),
			utils.ConvertToString(config.GetKeySize(), 0),
			config)
		s.Start(port)
	} else {
		s := server.NewServer(serverId, configFile)
		s.Start()
	}
}

func parseArgs() {
	flag.BoolVar(
		&isDebug,
		"d",
		false,
		"debug mode",
	)

	flag.IntVar(
		&serverId,
		"i",
		-1,
		"server id",
	)

	flag.StringVar(
		&configFile,
		"c",
		"",
		"server configuration file",
	)

	flag.StringVar(
		&cpuProfile,
		"cpuprofile",
		"",
		"write cpu profile to `file`")

	flag.Parse()

	if serverId == -1 {
		flag.Usage()
		logrus.Fatal("Invalid server id.")
	}
	if configFile == "" {
		flag.Usage()
		logrus.Fatal("Invalid configuration file.")
	}

}
