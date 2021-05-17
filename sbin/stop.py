#!/usr/bin/python
import argparse
import json

from paramiko import SSHClient, AutoAddPolicy

arg_parser = argparse.ArgumentParser(description="stop exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=True)
args = arg_parser.parse_args()

# Reads configurations
config_file = open(args.config, "r")
config = json.load(config_file)
config_file.close()

dynamic_latency = config["experiment"]["dynamicLatency"]["mode"]
use_timestamp = config["experiment"]["networkTimestamp"]
turn_on_network_measure = dynamic_latency and use_timestamp
ssh_username = config["experiment"]["ssh"]["username"]

def stop_servers():
    server_id = 0
    for ip in config["servers"]["machines_pub"]:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip, username=ssh_username)
        server_dir = config["experiment"]["runDir"] + "/server-" + str(server_id)
        # cmd = "killall -9 carousel-server; cd " + server_dir + "; rm -r raft-*; rm -rf *.log"
        cmd = "killall -9 carousel-server; cd " + server_dir + "; rm -r raft-*"
        print(cmd + " # at " + ip)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())
        server_id = server_id + 1


def stop_clients():
    for ip in config["clients"]["machines_pub"]:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip, username=ssh_username)
        client_dir = config["experiment"]["runDir"] + "/client"
        # cmd = "killall -9 client; cd " + client_dir + "; rm -rf *.log; rm -rf *.statistic"
        cmd = "killall -9 client"
        print(cmd + " # at " + ip)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())


def stop_networkMeasure():
    for ip in config["clients"]["networkMeasureMachines_pub"]:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip, username=ssh_username)
        client_dir = config["experiment"]["runDir"] + "/client"
        # cmd = "killall -9 client; cd " + client_dir + "; rm -rf *.log; rm -rf *.statistic"
        cmd = "killall -9 networkMeasure"
        print(cmd + " # at " + ip)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())


def main():
    stop_clients()
    if turn_on_network_measure:
        stop_networkMeasure()
    stop_servers()


if __name__ == "__main__":
    main()
