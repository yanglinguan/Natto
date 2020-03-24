#!/usr/bin/python
import json
import subprocess
import os
import argparse

from paramiko import SSHClient, AutoAddPolicy
from scp import SCPClient

path = os.getcwd()

arg_parser = argparse.ArgumentParser(description="run exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=False)
arg_parser.add_argument('-d', '--debug', help="turn on debug",
                        action='store_true')

args = arg_parser.parse_args()

bin_path = "/home/l69yang/Projects/go/src/Carousel-GTS/sbin/"


def run_exp(i):
    if args.config is not None:
        run(i, args.config)
    else:
        lists = os.listdir(path)
        for f in lists:
            if f.endswith(".json"):
                run(i, f)


def run(i, f):
    # print("run " + f + " " + str(i))
    if args.debug:
        subprocess.call([bin_path + "run.py", "-d", "-c", f])
    else:
        subprocess.call([bin_path + "run.py", "-c", f])
    dir_name = f.split('.')[0] + "-" + str(i)
    move_log(dir_name, f)


def remove_log(dir_path):
    lists = os.listdir(dir_path)
    for f in lists:
        if f.endswith(".log"):
            os.remove(os.path.join(dir_path, f))


def move_log(dir_name, f):
    config_file = open(os.path.join(path, f), "r")
    config = json.load(config_file)
    config_file.close()

    new_dir = os.path.join(path, dir_name)
    if os.path.isdir(new_dir):
        remove_log(new_dir)
    else:
        os.mkdir(new_dir)

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
        scp = SCPClient(ssh.get_transport())
        for sId in server_machine[mId]:
            server_dir = config["experiment"]["runDir"] + "/server-" + str(sId)
            scp.get(server_dir + "/*.log", new_dir)
            ssh.exec_command("rm -r " + server_dir + "/*")

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
        client_dir = config["experiment"]["runDir"] + "/client"
        scp = SCPClient(ssh.get_transport())
        scp.get(client_dir + "/*.log", new_dir)
        scp.get(client_dir + "/*.statistic", new_dir)
        ssh.exec_command("rm " + client_dir + "/*")


def main():
    for i in range(1):
        run_exp(i)


if __name__ == "__main__":
    main()
