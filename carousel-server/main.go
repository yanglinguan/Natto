package main

import (
	"Carousel-GTS/configuration"
	"Carousel-GTS/server"
	"Carousel-GTS/spanner"
	"Carousel-GTS/tapir"
	"Carousel-GTS/utils"
	"flag"
	"github.com/sirupsen/logrus"
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

	config := configuration.NewFileConfiguration(configFile)
	serverMode := config.GetServerMode()
	switch serverMode {
	case configuration.TAPIR:
		s := tapir.NewServer(strconv.Itoa(serverId), config.GetQueueLen(), false, 0)
		addr := config.GetServerAddressByServerId(serverId)
		port := strings.Split(addr, ":")[1]
		pId := config.GetPartitionIdByServerId(serverId)
		s.InitData(
			config.GetKeyListByPartitionId(pId),
			utils.ConvertToString(config.GetKeySize(), 0),
			config)
		s.Start(port)
	case configuration.SPANNER:
		s := spanner.NewServer(serverId, config)
		pId := config.GetPartitionIdByServerId(serverId)
		if config.GetWorkLoad() == configuration.SMALLBANK {
			keys := config.GetKeyListByPartitionId(pId)
			for _, key := range keys {
				s.InitKeyValue(key+config.GetSbSavingsAccountFlag(), config.GetSbInitBalance())
				s.InitKeyValue(key+config.GetSbCheckingAccountFlag(), config.GetSbInitBalance())
			}
		} else {
			s.InitData(config.GetKeyListByPartitionId(pId))
		}
		s.Start()
	default:
		s := server.NewServer(serverId, configFile, cpuProfile)
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
