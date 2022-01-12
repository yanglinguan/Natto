#!/usr/bin/python3

import argparse
import threading

import utils
import os
import time

arg_parser = argparse.ArgumentParser(description="tcp exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=True)
arg_parser.add_argument('-d', '--dir', dest='dir', nargs='?',
                        help='log store to dir', required=True)
args = arg_parser.parse_args()

# Reads configurations
config = utils.load_config(args.config)
run_dir = utils.get_run_dir(config)
leader_ips = ["10.0.3.1",
              "10.0.3.7",
              "10.0.3.10",
              "10.0.3.13",
              "10.0.2.10"]


def scp_tcp_log_exec(ssh, scp, ip):
    # client_dir = config["experiment"]["runDir"] + "/client"
    stdin, stdout, stderr = ssh.exec_command("ls " + run_dir + "/tcp*.log ")
    # stdin, stdout, stderr = ssh.exec_command("ls " + client_dir + "/*.statistic")
    log_files = stdout.read().split()
    for log in log_files:
        scp.get(log, args.dir)
    ssh.exec_command("rm " + run_dir + "/tcp*.log")
    print("collect tcp log from " + ip)


def ssh_exec_thread(ssh_client, ip, command):
    print("exec " + command + " # at " + ip)
    stdin, stdout, stderr = ssh_client.exec_command(command)
    print("finish " + command + " # at " + ip)
    print(stdout.read())
    print(stderr.read())


def collect_log(machines_server):
    threads = []
    for ip, server in machines_server.items():
        if ip not in leader_ips:
            continue
        thread = threading.Thread(
            target=scp_tcp_log_exec,
            args=(server.ssh_client, server.scp_client, ip)
        )
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()

def exec_cmd(ssh_client, cmd):
    ssh_client.exec_command(cmd)


def main():
    machines_server = utils.parse_server_machine(config)
    utils.create_ssh_client([machines_server])
    if os.path.isdir(args.dir):
        os.system("rm -rf " + args.dir + "/*")
    else:
        os.mkdir(args.dir)

    for sip, server in machines_server.items():
        if sip not in leader_ips:
            continue
        for cip, client in machines_server.items():
            if cip == sip or cip not in leader_ips:
                continue
            cmd = "iperf3 -s"
            th = threading.Thread(target=exec_cmd, args=(server.ssh_client, cmd))
            th.start()
            print("execed " + cmd + " # at " + sip)
            time.sleep(2)
            log_file = run_dir + "tcp-" + sip + "-" + cip + ".log"
            cmd = "ssh " + cip + " \"" + "iperf3 -c " + sip + " -t 250 -i /tmp > " + log_file + "\""
            print("exec " + cmd)
            os.system(cmd)
            print("finish " + cmd)
            server.ssh_client.exec_command("killall -9 iperf3")
            time.sleep(2)
    collect_log(machines_server)


if __name__ == "__main__":
    main()
