#!/usr/bin/python
import argparse
import json
import time
import threading
import os

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

path="/home/l69yang/Projects/go/src/Carousel-GTS/exp"
binPath="$HOME/Projects/go/bin/"
#binPath = "./"


def ssh_exec_thread(ssh_client, command):
    stdin, stdout, stderr = ssh_client.exec_command(command)
    print(stdout.read())
    print(stderr.read())
    

def start_servers():
    for serverId, info in config["servers"].items():
        ip = info["ip"]
        print(ip)
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "cd " + path + ";" + binPath + "carousel-server -d -i " + serverId + " -c configTest.json" +" > " + serverId +".log &"
        #cmd = "cd " + path + ";" + binPath + "carousel-server"
        print(cmd)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())

def start_clients():
    threads = list()
    for clientId, info in config["clients"].items():
        ip = info["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "cd " + path + ";" + binPath + "client -d -i " + clientId + " -c configTest.json" +" > " + clientId +".log"
        print(cmd)
        thread = threading.Thread(target=ssh_exec_thread, args=(ssh, cmd))
        threads.append(thread)
        thread.start()

    absolute_time = time.time() + config["experiment"]["duration"] + 10
    for thread in threads:
        timeout = absolute_time - time.time()
        if timeout < 0:
            break;
        thread.join(timeout)
        if thread.isAlive():
            print("Error: timeout")

def stop_servers():
    for serverId, info in config["servers"].items():
        ip = info["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "killall -9 carousel-server"
        print(cmd + " # at " + ip)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())

def stop_clients():
    for clientId, info in config["clients"].items():
        ip = info["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "killall -9 client"
        print(cmd + " # at " + ip)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())


def remove_log():
    lists = os.listdir(path)
    for f in lists:
        if f.endswith(".log"):
            os.remove(os.path.join(path, f))

def main():
    remove_log()
    start_servers()
    time.sleep(2)
    start_clients()
    stop_clients()
    stop_servers()

if __name__ == "__main__":
    main()
