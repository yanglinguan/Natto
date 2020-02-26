package utils

import (
	log "github.com/sirupsen/logrus"
	"os"
)

func ConfigLogger(isDebug bool) {
	log.SetFormatter(&log.JSONFormatter{})

	log.SetReportCaller(true)
	log.SetOutput(os.Stdout)

	log.SetLevel(log.WarnLevel)

	if isDebug {
		log.SetLevel(log.DebugLevel)
	}

}
