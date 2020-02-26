package utils

import (
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"os"
)

func ConfigLogger(isDebug bool, id string) {
	log.SetFormatter(&log.JSONFormatter{})

	log.SetReportCaller(true)
	log.SetOutput(os.Stdout)

	pathMap := lfshook.PathMap{
		log.InfoLevel:  id + "_info.log",
		log.ErrorLevel: id + "_error.log",
		log.DebugLevel: id + "_debug.log",
	}

	log.AddHook(lfshook.NewHook(pathMap, &log.JSONFormatter{}))

	log.SetLevel(log.WarnLevel)

	if isDebug {
		log.SetLevel(log.DebugLevel)
	}

}
