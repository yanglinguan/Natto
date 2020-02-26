package main

import (
	"Carousel-GTS/client"
	"Carousel-GTS/utils"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"log"
	"strconv"
	"sync"
)

var isDebug = false
var clientId = ""
var configFile = ""

var wg sync.WaitGroup

func main() {
	parseArgs()
	utils.ConfigLogger(isDebug, clientId)

	c := client.NewClient(clientId, configFile)

	for i := 0; i < 20; i++ {
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
	readKeyList, writeKeyList := getTxn(totalKey, 3, client.GetKeySize())
	readResult, txnId := client.ReadAndPrepare(readKeyList, writeKeyList)
	writeKeyValue := make(map[string]string)
	for _, wk := range writeKeyList {
		if value, exist := readResult[wk]; exist {
			var i int
			_, err := fmt.Sscan(value, &i)
			if err != nil {
				log.Fatalf("key %v invalid ", value)
			}
			i++
			writeKeyValue[wk] = convertToString(client.GetKeySize(), i)
		} else {
			writeKeyValue[wk] = wk
		}
	}

	for k, v := range writeKeyValue {
		logrus.Infof("write key %v: %v", k, v)
	}

	client.Commit(writeKeyValue, txnId)
	wg.Done()
}

func getTxn(totalKey int, txnSize int, keySize int) ([]string, []string) {
	//readKeyList := make([]string, txnSize)
	txnSize = totalKey
	readKeyList := make([]string, 0)
	writeKeyList := make([]string, txnSize)
	for i := 0; i < txnSize; i++ {
		key := i
		//key := rand.Intn(totalKey)
		keyStr := convertToString(keySize, key)
		//readKeyList[i] = keyStr
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
