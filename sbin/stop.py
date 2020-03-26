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


def stop_servers():
    server_id = 0
    for ip in config["servers"]["machines"]:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        server_dir = config["experiment"]["runDir"] + "/server-" + server_id
        cmd = "killall -9 carousel-server; cd " + server_dir + "; rm -r raft-*"
        print(cmd + " # at " + ip)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())
        server_id = server_id + 1


def stop_clients():
    for ip in config["clients"]["machines"]:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "killall -9 client"
        print(cmd + " # at " + ip)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())


def main():
    stop_clients()
    stop_servers()


if __name__ == "__main__":
    main()
