package main

import (
	"Carousel-GTS/client"
	"Carousel-GTS/configuration"
	"Carousel-GTS/utils"
	"flag"
	"github.com/sirupsen/logrus"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var carouselClient *client.Client
var wg sync.WaitGroup
var carouselServerCmd = "./carousel-server "

var IsDebug bool
var ConfigFile string
var isLocalMode bool
var waitTime = 10

func main() {

	ParseArgs()
	utils.ConfigLogger(IsDebug)
	//ParseExpSettingFile(ExpSettingFile)
	//ParseServerConfig(ServerLocationConfigFile)
	if IsDebug {
		carouselServerCmd += "-d -i "
	} else {
		carouselServerCmd += "-i "
	}

	carouselClient = client.NewClient(0, ConfigFile)

	for partitionId, leaderId := range carouselClient.Config.GetExpectPartitionLeaders() {
		logrus.Debugf("Enforcing partition %v leader to be server Id = %v", partitionId, leaderId)

		wg.Add(1)

		go EnforceLeader(leaderId, carouselClient.Config)
	}

	wg.Wait()

	for _, expectedLeaderServerId := range carouselClient.Config.GetExpectPartitionLeaders() {
		carouselClient.StartProbe(expectedLeaderServerId)
	}
}

func ParseArgs() {
	flag.BoolVar(
		&IsDebug,
		"d",
		false,
		"enables debug mode",
	)
	flag.StringVar(
		&ConfigFile,
		"c",
		"",
		"Carousel config .json configuration file <REQUIRED>",
	)
	flag.BoolVar(
		&isLocalMode,
		"local",
		false,
		"run server on local machine",
	)

	flag.Parse()

	if ConfigFile == "" {
		logrus.Fatal("Invalid arguments")
		flag.Usage()
	}
}

// Enforces a server to be the leader of an Raft instance
func EnforceLeader(expectedLeaderServerId int, config configuration.Configuration) {

	defer wg.Done()

	//expectedLeaderServerId := serverAddrToInfoTable[expectedLeaderServerId].id

	logrus.Infof("Expects the leader to be server Id = %v, addr = %v",
		expectedLeaderServerId, config.GetServerAddressByServerId(expectedLeaderServerId))

	// Sends a heart beat message to check if the leader is the expected server.
	// The hear-beat response contains the current leader's server address.
	curLeaderId := carouselClient.HeartBeat(expectedLeaderServerId)

	logrus.Infof("The current leader is server id = %v expect leaderId %v", curLeaderId, expectedLeaderServerId)

	// Kills the current leader until the expected server becomes the leader
	for curLeaderId != expectedLeaderServerId {

		logrus.Debugf("Killing the current leader, server id %v expect leaderId %v", curLeaderId, expectedLeaderServerId)

		if isLocalMode {
			StopLocalServer(curLeaderId, config)
		} else {
			StopRemoteServer(curLeaderId, config)
		}
		// Waits for Raft to elect a new leader
		time.Sleep(time.Duration(waitTime) * time.Second)

		logrus.Debugf("Starting the server Id = %v", curLeaderId)

		StartServer(config, curLeaderId)
		// Waits for the server to join in the Raft instance
		time.Sleep(time.Duration(waitTime) * time.Second)

		curLeaderId = carouselClient.HeartBeat(expectedLeaderServerId)
		if curLeaderId == -1 {
			logrus.Fatalf("Invalid current leader address. Expected leader addr = %v", expectedLeaderServerId)
		}

		logrus.Infof("The current leader is server id = %v, expected leader id = %v", curLeaderId, expectedLeaderServerId)
	}
}

// Stops a server that runs on the local machine
func StopLocalServer(serverId int, config configuration.Configuration) {
	//serverInfo := serverAddrToInfoTable[serverAddr]
	//serverId := serverInfo.id
	pid := getLocalPid(serverId)
	if len(pid) == 0 {
		logrus.Fatalf("Cannot stop server id = %v due to failing to locate the process on the local machine", serverId)
	}

	serverDir := config.GetRunDir() + "/server-" + strconv.Itoa(serverId)

	serverLogFile := "server-" + strconv.Itoa(serverId) + ".log"
	cmd := "cd " + serverDir + "; " +
		"kill " + pid + "; " +
		"rm " + serverLogFile + "; " +
		"rm -r raft-*-snap raft-*-wal;"

	execBashCmd(cmd)
}

// Stops a carousel server
func StopRemoteServer(serverId int, config configuration.Configuration) {
	//serverInfo := serverAddrToInfoTable[serverAddr]
	//serverId := serverInfo.id
	sIdStr := strconv.Itoa(serverId)
	pid := getRemotePid(serverId, config)
	serverDir := config.GetRunDir() + "/server-" + sIdStr

	serverLogFile := "server-" + sIdStr + ".log"
	cmd := "cd " + serverDir + "; " +
		"kill " + pid + "; " +
		"rm " + serverLogFile + "; " +
		"rm -r raft-*-snap raft-*-wal;"
	//cmd := "cd " + serverDir + "; " +
	//	"killall carousel-server; " +
	//	"rm " + serverLogFile + "; " +
	//	"rm -r raft-*-snap raft-*-wal;"

	//// Using a pid file may not work when servers start as background processes
	////serverPidFile := "server-" + serverId + ".pid"
	////cmd := "cd " + serverDir + "; " +
	////	"pid=\\`cat " + serverPidFile + "\\`; kill \\$pid; " +
	////	"rm " + serverLogFile + "; " +
	////	"rm -r raft-*-snap raft-*-wal;"

	sshCmd := buildSshCmd(config, serverId, cmd)
	execBashCmd(sshCmd)
}

// Starts a carousel server
func StartServer(config configuration.Configuration, serverId int) {
	sIdStr := strconv.Itoa(serverId)
	serverDir := config.GetRunDir() + "/server-" + sIdStr

	serverLogFile := "server-" + sIdStr + ".log"

	cmd := "ulimit -c unlimited;ulimit -n 100000; cd " + serverDir + "; " + carouselServerCmd + sIdStr + " -c " + ConfigFile +
		" > " + serverLogFile + " 2>&1 & "

	sshCmd := buildSshCmd(config, serverId, cmd)
	execBashCmd(sshCmd)
}

func buildSshCmd(config configuration.Configuration, serverId int, cmd string) string {
	sshCmd := strings.Split(config.GetServerAddressByServerId(serverId), ":")[0]
	username := config.GetSSHUsername()
	if len(username) != 0 {
		sshCmd = username + "@" + sshCmd
	}
	identity := config.GetSSHIdentity()
	if len(identity) != 0 {
		sshCmd = "-i " + identity + " " + sshCmd
	}
	sshCmd = "ssh " + sshCmd + " " + "\"" + cmd + "\""

	return sshCmd
}

// Runs the command in a local bash, which will evaluate environment variables
func execBashCmd(cmd string) string {
	logrus.Debugf("Executing command: %v", cmd)

	shell := exec.Command("bash", "-c", cmd)
	stdoutStderr, err := shell.CombinedOutput()
	if err != nil {
		logrus.Errorf("Error: %v from\n %v", err, string(stdoutStderr))
	}

	logrus.Debugf(string(stdoutStderr))

	return string(stdoutStderr)
}

func getLocalPid(serverId int) string {
	cmd := exec.Command("pgrep", "-f", carouselServerCmd+strconv.Itoa(serverId))
	pid := ""
	if stdout, err := cmd.Output(); err != nil {
		logrus.Errorf("Failed to get process pid for server id = %v, error = %v", serverId, err)
	} else {
		pid = strings.TrimSuffix(string(stdout), "\n")
	}

	if len(pid) == 0 {
		logrus.Errorf("There is no process for server id = %v", serverId)
	}

	logrus.Debugf("server id = %v, pid = %v", serverId, pid)

	return pid
}

func getRemotePid(serverId int, config configuration.Configuration) string {
	//serverInfo := serverAddrToInfoTable[serverAddr]
	//serverId := serverInfo.id
	cmd := "pgrep -f " + "\\\"" + carouselServerCmd + strconv.Itoa(serverId) + "\\\""
	sshCmd := buildSshCmd(config, serverId, cmd)
	pid := execBashCmd(sshCmd)
	if len(pid) == 0 {
		logrus.Errorf("There is no process for server id = %v", serverId)
	}
	pid = strings.TrimSuffix(pid, "\n")
	return pid
}
