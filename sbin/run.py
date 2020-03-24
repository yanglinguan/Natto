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
check_server_status_cmd = binPath + "/checkServerStatus "
enforce_leader_cmd = binPath + "/enforce-leader "

if args.debug:
    server_cmd = server_cmd + "-d "
    client_cmd = client_cmd + "-d "
    check_server_status_cmd = check_server_status_cmd + "-d "
    enforce_leader_cmd = enforce_leader_cmd + "-d "

src_path = "$HOME/Projects/go/src/Carousel-GTS/"
server_path = src_path + "carousel-server/"
client_path = src_path + "benchmark/client/"
rpc_path = src_path + "rpc/"
tool_path = src_path + "tools/"
check_server_status_path = tool_path + "checkServerStatus/"
enforce_leader_path = tool_path + "enforce-leader/"


def ssh_exec_thread(ssh_client, command, server):
    stdin, stdout, stderr = ssh_client.exec_command(command)
    print(stdout.read())
    print(stderr.read())
    print("clients on " + server + " finishes")


def start_servers():
    server_nums = config["servers"]["nums"]
    machines = config["servers"]["machines"]
    server_machine = [[] for i in range(len(machines))]
    for server_id in range(server_nums):
        idx = server_id % len(machines)
        server_machine[idx].append(str(server_id))
    for mId in range(len(server_machine)):
        if len(server_machine[mId]) == 0:
            continue
        m = machines[mId]
        ip = m["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        cmd += "cd " + path + ";"
        exe = "cd " + path + "; mkdir -p server-$id; cd server-$id; cp " + path + "/" + args.config + " ./; " + \
              server_cmd + "-i $id" + " -c " + args.config + " > " + "server-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(server_machine[mId]) + "; do " + exe + " done"
        cmd += loop
        print(cmd + " # at " + ip)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())
        print("server " + ' '.join(server_machine[mId]) + " is running on machine " + ip)


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
        cmd += "cd " + path + "; mkdir -p client;" + " cp " + path + "/" + args.config + " " + path + "/client/; "
        exe = "cd " + path + "/client;" + \
              client_cmd + "-i $id" + " -c " + args.config + " > " + "client-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(client_machine[mId]) + "; do " + exe + " done; wait"
        cmd += loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(ssh, cmd, ip))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def print_server_status():
    cmd = check_server_status_cmd + "-c " + args.config
    subprocess.call(cmd, shell=True)


def enforce_leader():
    if config["experiment"]["replication"]:
        cmd = enforce_leader_cmd + "-c " + args.config
        subprocess.call(cmd, shell=True)


def stop_servers():
    for machine in config["servers"]["machines"]:
        ip = machine["ip"]
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
    subprocess.call("rm -r server-*/*", shell=True)
    subprocess.call("rm client/*", shell=True)


def build():
    try:
        print("build server at " + server_path)
        subprocess.call("cd " + rpc_path + "; protoc --go_out=plugins=grpc:. *.proto", shell=True)
        subprocess.call("cd " + server_path + "; go install", shell=True)
        print("build client at " + client_path)
        subprocess.call("cd " + client_path + "; go install", shell=True)
        print("build tool check server status at " + check_server_status_path)
        subprocess.call("cd " + check_server_status_path + "; go install", shell=True)
        print("build tool enforce leader at " + enforce_leader_path)
        subprocess.call("cd " + enforce_leader_path + "; go install", shell=True)
        subprocess.call("cd " + path, shell=True)
    except subprocess.CalledProcessError:
        print("build error")


def main():
    remove_log(path)
    build()
    start_servers()
    time.sleep(15)
    enforce_leader()
    start_clients()
    print_server_status()
    stop_clients()
    stop_servers()


if __name__ == "__main__":
    main()
