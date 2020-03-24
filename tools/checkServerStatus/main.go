package main

import (
	"Carousel-GTS/client"
	"Carousel-GTS/utils"
	"bufio"
	"flag"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
)

var isDebug = false
var configFile = ""

func main() {
	parseArgs()
	utils.ConfigLogger(isDebug)

	c := client.NewClient(0, configFile)

	committedTxn := parseClientLog(c)

	c.PrintServerStatus(committedTxn)
}

func parseClientLog(client *client.Client) []int {
	root, err := os.Getwd()
	if err != nil {
		logrus.Fatalf("cannot get path %v", err)
	}

	var files []string
	_ = filepath.Walk(root+"/client", func(path string, f os.FileInfo, _ error) error {
		if !f.IsDir() {
			if filepath.Ext(f.Name()) == ".statistic" {
				logrus.Debugf("path %v file %v", path, f.Name())
				files = append(files, path)
			}
		}
		return nil
	})

	result := make([]int, client.Config.GetTotalPartition())
	for _, fName := range files {
		lines := readFile(fName)
		logrus.Debugf("file %v lines %v", fName, len(lines))
		for _, line := range lines {
			items := strings.Split(line, ",")
			commitResult := items[1]
			if commitResult != "1" {
				continue
			}
			keys := strings.Split(items[5][1:len(items[5])-1], " ")
			partitionSet := make(map[int]bool)
			for _, key := range keys {
				pId := client.Config.GetPartitionIdByKey(key)
				partitionSet[pId] = true
			}

			for pId := range partitionSet {
				result[pId]++
			}
		}
	}

	return result
}

func readFile(filePath string) []string {
	file, err := os.Open(filePath)
	if err != nil {
		logrus.Fatal(err)
	}
	defer file.Close()
	lineList := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || len(line) == 0 {
			continue
		}
		lineList = append(lineList, line)
	}

	if err := scanner.Err(); err != nil {
		logrus.Fatal(err)
	}

	return lineList
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
