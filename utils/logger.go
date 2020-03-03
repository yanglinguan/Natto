package utils

import (
	//"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"os"
)

func ConfigLogger(isDebug bool, id string) {
	formatter := new(log.TextFormatter)
	formatter.TimestampFormat = "Jan _2 15:04:05.000000"
	formatter.FullTimestamp = true
	//formatter.ForceColors = true
	log.SetFormatter(formatter)
	//log.SetFormatter(&log.JSONFormatter{})
	//log.SetFormatter(&log.TextFormatter{
	//	FullTimestamp: true,
	//})

	log.SetReportCaller(true)
	log.SetOutput(os.Stdout)

	//pathMap := lfshook.PathMap{
	//	log.InfoLevel:  id + "_info.log",
	//	log.FatalLevel: id + "_fatal.log",
	//	log.DebugLevel: id + "_debug.log",
	//}

	//log.AddHook(lfshook.NewHook(pathMap, formatter))

	log.SetLevel(log.WarnLevel)

	if isDebug {
		log.SetLevel(log.DebugLevel)
	}

}
