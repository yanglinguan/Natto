#!/usr/bin/python
import argparse
import json
import time
import threading
import os
import subprocess

from paramiko import SSHClient, AutoAddPolicy
from scp import SCPClient

arg_parser = argparse.ArgumentParser(description="run exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=True)
arg_parser.add_argument('-d', '--debug', help="turn on debug",
                        action='store_true')
arg_parser.add_argument('-r', '--runCount', dest='runCount', nargs='?',
                        help='runCount', required=True)

args = arg_parser.parse_args()

# Reads configurations
config_file = open(args.config, "r")
config = json.load(config_file)
config_file.close()

path = os.getcwd()
binPath = "/home/l69yang/Projects/go/bin/"

server_cmd = "./carousel-server "
client_cmd = "./client "
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

if "runDir" in config["experiment"] and len(config["experiment"]["runDir"]) != 0:
    path = config["experiment"]["runDir"]


def parse_client_machine():
    client_nums = config["clients"]["nums"]
    machines = config["clients"]["machines"]
    client_machine = [[] for i in range(len(machines))]
    for clientId in range(client_nums):
        idx = clientId % len(machines)
        client_machine[idx].append(str(clientId))
    return client_machine, machines


def parse_server_machine():
    server_nums = config["servers"]["nums"]
    machines = config["servers"]["machines"]
    server_machine = [[] for i in range(len(machines))]
    for server_id in range(server_nums):
        idx = server_id % len(machines)
        server_machine[idx].append(str(server_id))
    return server_machine, machines


def collect_client_log(clients, machines):
    dir_name = args.config.split('.')[0] + "-" + args.runCount
    new_dir = os.path.join(os.getcwd(), dir_name)
    if os.path.isdir(new_dir):
        subprocess.call("rm -r " + new_dir + "/*", shell=True)
    else:
        os.mkdir(new_dir)

    for mId in range(len(clients)):
        if len(clients[mId]) == 0:
            continue
        m = machines[mId]
        ip = m["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        scp = SCPClient(ssh.get_transport())
        client_dir = config["experiment"]["runDir"] + "/client"
        stdin, stdout, stderr = ssh.exec_command("ls " + client_dir + "/*.log " + client_dir + "/*.statistic")
        log_files = stdout.read().split()
        for log in log_files:
            scp.get(log, new_dir)
        ssh.exec_command("rm " + client_dir + "/*.log")
        ssh.exec_command("rm " + client_dir + "/*.statistic")
        print("collect client log from " + ip)
    return new_dir


def collect_server_log(new_dir, servers, machines):
    for mId in range(len(servers)):
        if len(servers[mId]) == 0:
            continue
        m = machines[mId]
        ip = m["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        scp = SCPClient(ssh.get_transport())
        for sId in servers[mId]:
            server_dir = config["experiment"]["runDir"] + "/server-" + str(sId)
            stdin, stdout, stderr = ssh.exec_command("ls " + server_dir + "/*.log")
            log_files = stdout.read().split()
            for log in log_files:
                scp.get(log, new_dir)
            ssh.exec_command("rm -r " + server_dir + "/raft-*")
            ssh.exec_command("rm -r " + server_dir + "/*.log")
            print("collect server log " + sId + " from " + ip)


def scp_server_exec(ssh, scp, servers, mid, ip):
    for sId in servers[mid]:
        server_dir = config["experiment"]["runDir"] + "/server-" + str(sId)
        ssh.exec_command("mkdir -p " + server_dir)
        scp.put(os.getcwd() + "/" + args.config, server_dir)
        scp.put(binPath + "carousel-server", server_dir)
        print("deploy config and server " + str(sId) + " at " + ip)


def scp_client_exec(ssh, scp, ip):
    client_dir = config["experiment"]["runDir"] + "/client"
    ssh.exec_command("mkdir -p " + client_dir)
    scp.put(os.getcwd() + "/" + args.config, client_dir)
    scp.put(binPath + "client", client_dir)
    print("deploy config and client at " + ip)


def deploy(servers, server_machines, clients, client_machines):
    if "runDir" not in config["experiment"] or len(config["experiment"]["runDir"]) == 0:
        return

    threads = []
    for mId in range(len(servers)):
        if len(servers[mId]) == 0:
            continue
        m = server_machines[mId]
        ip = m["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        scp = SCPClient(ssh.get_transport())
        thread = threading.Thread(target=scp_server_exec, args=(ssh, scp, servers, mId, ip))
        threads.append(thread)
        thread.start()

    for mId in range(len(clients)):
        if len(clients[mId]) == 0:
            continue
        m = client_machines[mId]
        ip = m["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        scp = SCPClient(ssh.get_transport())
        thread = threading.Thread(target=scp_client_exec, args=(ssh, scp, ip))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def ssh_exec_thread(ssh_client, command, server):
    stdin, stdout, stderr = ssh_client.exec_command(command)
    print(stdout.read())
    print(stderr.read())
    print("clients on " + server + " finishes")


def start_servers(servers, machines):
    for mId in range(len(servers)):
        if len(servers[mId]) == 0:
            continue
        m = machines[mId]
        ip = m["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        # cmd += "cd " + path + ";"
        exe = "cd " + path + "/server-$id; " + \
              server_cmd + "-i $id" + " -c " + args.config + " > " + "server-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(servers[mId]) + "; do " + exe + " done"
        cmd += loop
        print(cmd + " # at " + ip)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read())
        print(stderr.read())
        print("server " + ' '.join(servers[mId]) + " is running on machine " + ip)


def start_clients(clients, machines):
    threads = list()

    for mId in range(len(clients)):
        if len(clients[mId]) == 0:
            continue
        m = machines[mId]
        ip = m["ip"]
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        # cmd += "cd " + path + "; mkdir -p client;" + " cp " + path + "/" + args.config + " " + path + "/client/; "
        exe = "cd " + path + "/client;" + \
              client_cmd + "-i $id" + " -c " + args.config + " > " + "client-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(clients[mId]) + "; do " + exe + " done; wait"
        cmd += loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(ssh, cmd, ip))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def print_server_status(dir_name):
    cmd = check_server_status_cmd + "-c " + args.config + " -r " + dir_name
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
        subprocess.call("cd " + os.getcwd(), shell=True)
    except subprocess.CalledProcessError:
        print("build error")


def main():
    build()
    servers, server_machines = parse_server_machine()
    clients, client_machines = parse_client_machine()
    deploy(servers, server_machines, clients, client_machines)
    start_servers(servers, server_machines)
    time.sleep(15)
    enforce_leader()
    start_clients(clients, client_machines)
    dir_name = collect_client_log(clients, client_machines)
    print_server_status(dir_name)
    collect_server_log(dir_name, servers, server_machines)
    stop_clients()
    stop_servers()


if __name__ == "__main__":
    main()
