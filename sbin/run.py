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
arg_parser.add_argument('-d', '--debug', help="turn on debug",
                        action='store_true')

args = arg_parser.parse_args()

# Reads configurations
config_file = open(args.config, "r")
config = json.load(config_file)
config_file.close()

path = "/home/l69yang/Projects/go/src/Carousel-GTS/exp"
binPath = "$HOME/Projects/go/bin/"

server_cmd = binPath + "/carousel-server "
client_cmd = binPath + "/client "
if args.debug:
    server_cmd = server_cmd + "-d "
    client_cmd = client_cmd + "-d "


# binPath = "./"


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
        cmd = "cd " + path + ";" + server_cmd + "-i " + \
              serverId + " -c " + args.config + " > " + serverId + ".log &"
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
        cmd = "cd " + path + ";" + client_cmd + "-i " + clientId + \
              " -c " + args.config + " > " + clientId + ".log"
        print(cmd)
        thread = threading.Thread(target=ssh_exec_thread, args=(ssh, cmd))
        threads.append(thread)
        thread.start()

    # absolute_time = time.time() + config["experiment"]["duration"] + 10
    for thread in threads:
        # timeout = absolute_time - time.time()
        # if timeout < 0:
        #    break
        # thread.join(timeout)
        # if thread.isAlive():
        #    print("Error: timeout")
        thread.join()


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


def remove_log(dir_path):
    lists = os.listdir(dir_path)
    for f in lists:
        if f.endswith(".log"):
            os.remove(os.path.join(dir_path, f))


def main():
    remove_log(path)
    start_servers()
    time.sleep(2)
    start_clients()
    stop_clients()
    stop_servers()


if __name__ == "__main__":
    main()
