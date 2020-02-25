package main

import (
	"Carousel-GTS/client"
	"Carousel-GTS/utils"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"strconv"
	"sync"
)

var isDebug bool = false
var clientId string = ""
var configFile string = ""

var wg sync.WaitGroup

func main() {
	parseArgs()
	utils.ConfigLogger(isDebug)

	c := client.NewClient(clientId, configFile)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go execTxn(c)
	}
	wg.Wait()
	c.PrintTxnStatisticData()
}

func convertToString(size int, key int) string {
	format := "%" + strconv.Itoa(size) + "d"
	return fmt.Sprintf(format, key)
}

func execTxn(client *client.Client) {
	totalKey := client.GetKeyNum()
	readKeyList, writeKeyList := getTxn(totalKey, 4)
	readResult, txnId := client.ReadAndPrepare(readKeyList, writeKeyList)
	writeKeyValue := make(map[string]string)
	for _, wk := range writeKeyList {
		if value, exist := readResult[wk]; exist {
			var i int
			vInt, _ := fmt.Sscan(value, i)
			vInt++
			writeKeyValue[wk] = convertToString(64, vInt)
		} else {
			writeKeyValue[wk] = wk
		}
	}

	client.Commit(writeKeyValue, txnId)
	wg.Done()
}

func getTxn(totalKey int, txnSize int) ([]string, []string) {
	readKeyList := make([]string, txnSize)
	writeKeyList := make([]string, txnSize)
	for i := 0; i < txnSize; i++ {
		key := rand.Intn(totalKey)
		keyStr := convertToString(64, key)
		readKeyList[i] = keyStr
		writeKeyList[i] = keyStr
	}
	return readKeyList, writeKeyList
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
