#!/usr/bin/python
import argparse
import json
import time

from paramiko import SSHClient, AutoAddPolicy

arg_parser = argparse.ArgumentParser(description="run exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=True)

args = arg_parser.parse_args()

# Reads configurations
config_file = open(args.config, "r")
config = json.load(config_file)
config_file.close()

for serverId, info in config["servers"].items():
    ip = info["ip"]
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())
    ssh.connect(ip)
    ssh.exec_command("carousel-server -d -i " + serverId + " -c config.json &")

time.sleep(1)

for clientId, info in config["clients"].items():
    ip = info["ip"]
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())
    ssh.connect(ip)
    ssh.exec_command("client -d -i " + clientId + " -c config.json &")

for clientId, info in config["clients"].items():
    ip = info["ip"]
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())
    ssh.connect(ip)
    transport = ssh.get_transport()
    channel = transport.open_session()
    channel.exec_command('client')
    status = channel.recv_exit_status()
