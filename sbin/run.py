#!/usr/bin/python
import argparse
import datetime
import json
import time
import threading
import os
import subprocess
# from termcolor import colored

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
arg_parser.add_argument('-b', '--buildDeploy', help="build and deploy",
                        action='store_true')

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


class Machine:
    def __init__(self, machine_ip):
        self.machine_ip = machine_ip
        self.ids = []

    def add_id(self, server_id):
        self.ids.append(server_id)


machines_client = {}

machines_server = {}


def parse_client_machine():
    client_nums = config["clients"]["nums"]
    machines = config["clients"]["machines"]
    for ip in machines:
        machines_client[ip] = Machine(ip)
    for clientId in range(client_nums):
        idx = clientId % len(machines)
        ip = machines[idx]
        machines_client[ip].add_id(str(clientId))


def parse_server_machine():
    server_nums = int(config["servers"]["nums"])
    machines = config["servers"]["machines"]
    for ip in machines:
        machines_server[ip] = Machine(ip)
    for server_id in range(server_nums):
        idx = server_id % len(machines)
        ip = machines[idx]
        machines_server[ip].add_id(str(server_id))


def scp_client_log_exec(new_dir, ssh, scp, ip):
    client_dir = config["experiment"]["runDir"] + "/client"
    stdin, stdout, stderr = ssh.exec_command("ls " + client_dir + "/*.log " + client_dir + "/*.statistic")
    log_files = stdout.read().split()
    for log in log_files:
        scp.get(log, new_dir)
    ssh.exec_command("rm " + client_dir + "/*.log")
    ssh.exec_command("rm " + client_dir + "/*.statistic")
    print("collect client log from " + ip)


def collect_client_log():
    threads = list()
    dir_name = args.config.split('.')[0] + "-" + args.runCount
    new_dir = os.path.join(os.getcwd(), dir_name)
    if os.path.isdir(new_dir):
        subprocess.call("rm -r " + new_dir + "/*", shell=True)
    else:
        os.mkdir(new_dir)

    for ip, machine in machines_client.items():
        if len(machine.ids) == 0:
            continue
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        scp = SCPClient(ssh.get_transport())
        thread = threading.Thread(target=scp_client_log_exec, args=(new_dir, ssh, scp, ip))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return new_dir


def scp_server_log_exec(new_dir, ssh, scp, s_id, ip):
    server_dir = config["experiment"]["runDir"] + "/server-" + str(s_id)
    stdin, stdout, stderr = ssh.exec_command("ls " + server_dir + "/*.log")
    log_files = stdout.read().split()
    for log in log_files:
        scp.get(log, new_dir)
    ssh.exec_command("rm -r " + server_dir + "/raft-*")
    ssh.exec_command("rm -r " + server_dir + "/*.log")
    print("collect server log " + s_id + " from " + ip)


def collect_server_log(new_dir):
    threads = list()
    for ip, machine in machines_server.items():
        if len(machine.ids) == 0:
            continue
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        scp = SCPClient(ssh.get_transport())
        for sId in machine.ids:
            thread = threading.Thread(target=scp_server_log_exec, args=(new_dir, ssh, scp, sId, ip))
            threads.append(thread)
            thread.start()

    for thread in threads:
        thread.join()


def scp_server_exec(ssh, scp, s_id, ip):
    server_dir = config["experiment"]["runDir"] + "/server-" + str(s_id)
    ssh.exec_command("mkdir -p " + server_dir)
    scp.put(os.getcwd() + "/" + args.config, server_dir)
    scp.put(binPath + "carousel-server", server_dir)
    print("deploy config and server " + str(s_id) + " at " + ip)


def scp_client_exec(ssh, scp, ip):
    client_dir = config["experiment"]["runDir"] + "/client"
    ssh.exec_command("mkdir -p " + client_dir)
    scp.put(os.getcwd() + "/" + args.config, client_dir)
    scp.put(binPath + "client", client_dir)
    print("deploy config and client at " + ip)


def deploy():
    if "runDir" not in config["experiment"] or len(config["experiment"]["runDir"]) == 0:
        return

    threads = []
    for ip, machine in machines_server.items():
        if len(machine.ids) == 0:
            continue
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        scp = SCPClient(ssh.get_transport())
        for sId in machine.ids:
            thread = threading.Thread(target=scp_server_exec, args=(ssh, scp, sId, ip))
            threads.append(thread)
            thread.start()

    for ip, machine in machines_client.items():
        if len(machine.ids) == 0:
            continue
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        scp = SCPClient(ssh.get_transport())
        thread = threading.Thread(target=scp_client_exec, args=(ssh, scp, ip))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def ssh_exec_thread(ssh_client, command, ip, servers=None, stop=False):
    stdin, stdout, stderr = ssh_client.exec_command(command)
    print(stdout.read())
    print(stderr.read())
    if servers is None:
        if not stop:
            print("clients on " + ip + " finishes")
    else:
        if not stop:
            print("server " + ' '.join(servers) + " starts on " + ip)


def start_servers():
    threads = list()
    for ip, machine in machines_server.items():
        if len(machine.ids) == 0:
            continue
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        # cmd += "cd " + path + ";"
        exe = "cd " + path + "/server-$id; " + \
              server_cmd + "-i $id" + " -c " + args.config + " > " + "server-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done"
        cmd += loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(ssh, cmd, ip, machine.ids))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def start_clients():
    threads = list()

    start_time = str(int((time.time() + 25) * 1000 * 1000 * 1000))
    for ip, machine in machines_client.items():
        if len(machine.ids) == 0:
            continue
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        # cmd += "cd " + path + "; mkdir -p client;" + " cp " + path + "/" + args.config + " " + path + "/client/; "
        exe = "sleep 0.01; cd " + path + "/client;" + \
                client_cmd + "-i $id" + " -c " + args.config + " -t " + start_time + " > " + "client-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done; wait"
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
    if config["servers"]["replicationFactor"] > 1:
        cmd = enforce_leader_cmd + "-c " + args.config
        subprocess.call(cmd, shell=True)


def stop_servers():
    threads = list()
    for ip, machine in machines_server.items():
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        server_dir = path + "/server-$id"
        exe = "cd " + server_dir + "; " + "rm -r raft-*; rm -r *.log;"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done"
        cmd = "killall -9 carousel-server; " + loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(ssh, cmd, ip, ip, True))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def stop_clients():
    threads = list()
    for ip in config["clients"]["machines"]:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.connect(ip)
        cmd = "killall -9 client"
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(ssh, cmd, ip, ip, True))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


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
    start_time = time.time()
    parse_server_machine()
    parse_client_machine()
    end_deploy = start_time
    deploy_use = 0
    build_use = 0
    if args.buildDeploy:
        build()
        end_build = time.time()
        build_use = end_build - start_time
        print("build use %.5fs" % build_use)
        deploy()
        end_deploy = time.time()
        deploy_use = end_deploy - end_build
        print("deploy use %.5fs" % deploy_use)
    start_servers()
    time.sleep(15)
    end_start_server = time.time()
    start_server_use = end_start_server - end_deploy
    print("start server use (+15s) %.5fs" % start_server_use)
    enforce_leader()
    end_select_leader = time.time()
    select_leader_use = end_select_leader - end_start_server
    print("select leader used %.5fs" % select_leader_use)
    start_client_time = datetime.datetime.now().strftime("%H:%M:%S")
    print("start client at time " + start_client_time)
    start_clients()
    end_client = time.time()
    client_use = end_client - end_select_leader
    print("clients finish used %.5fs" % client_use)
    dir_name = collect_client_log()
    if args.debug:
    # dir_name = collect_client_log()
        print_server_status(dir_name)
        end_server = time.time()
        server_use = end_server - end_client
        print("server finish used %.5fs" % server_use)
        collect_server_log(dir_name)
    end_collect = time.time()
    collect_use = end_collect - end_client
    print("collect log used %.5fs" % collect_use)
    # stop_clients()
    stop_servers()
    end_time = time.time()
    stop_server_use = end_time - end_collect
    print("stop client and server use %.5f" % stop_server_use)
    print("-----")
    print("entire exp use %.5fs" % (end_time - start_time))
    print("build use %.5fs" % build_use)
    print("deploy use %.5fs" % deploy_use)
    print("start server use (+15s) %.5fs" % start_server_use)
    print("select leader used %.5fs" % select_leader_use)
    print("run clients used %.5fs" % client_use)
    # print("server finish used %.5fs" % server_use)
    # print("collect log used %.5fs" % collect_use)
    print("stop client and server use %.5f" % stop_server_use)


if __name__ == "__main__":
    main()
