#!/usr/bin/python
import argparse
import json
import time
import threading
import os
import subprocess

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

path = os.getcwd()
binPath = "$HOME/Projects/go/bin/"

server_cmd = binPath + "/carousel-server "
client_cmd = binPath + "/client "
if args.debug:
    server_cmd = server_cmd + "-d "
    client_cmd = client_cmd + "-d "

src_path = "$HOME/Projects/go/src/Carousel-GTS/"
server_path = src_path + "carousel-server/"
client_path = src_path + "benchmark/client/"
rpc_path = src_path + "rpc/"


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
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        cmd += "cd " + path + ";" + server_cmd + "-i " + \
               serverId + " -c " + args.config + " > " + serverId + ".log &"
        print(cmd)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())


def start_clients():
    threads = list()
    client_nums = config["clients"]["nums"]
    machines = config["clients"]["machines"]
    client_machine = [[] for i in range(len(machines))]
    for clientId in range(client_nums):
        idx = clientId % len(machines)
        client_machine[idx].append(str(clientId))

    for mId in range(len(client_machine)):
        if len(client_machine[mId]) == 0:
            continue
        m = machines[mId]
        ip = m["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        cmd += "cd " + path + ";"
        exe = client_cmd + "-i $id" + " -c " + args.config + " > " + " $id.log &"
        loop = "for id in " + ' '.join(client_machine[mId]) + "; do " + exe + " done; wait"
        cmd += loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(ssh, cmd))
        threads.append(thread)
        thread.start()

    for thread in threads:
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
    for info in config["clients"]["machines"]:
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


def build():
    try:
        print("build server at " + server_path)
        subprocess.call("cd " + rpc_path + "; protoc --go_out=plugins=grpc:. *.proto", shell=True)
        subprocess.call("cd " + server_path + "; go install", shell=True)
        print("build client at " + client_path)
        subprocess.call("cd " + client_path + "; go install", shell=True)
        subprocess.call("cd " + path, shell=True)
    except subprocess.CalledProcessError:
        print("build error")


def main():
    remove_log(path)
    build()
    start_servers()
    time.sleep(2)
    start_clients()
    stop_clients()
    stop_servers()


if __name__ == "__main__":
    main()
