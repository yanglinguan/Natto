#!/usr/bin/python
import argparse
import datetime
import time
import threading
import os
import subprocess
import utils

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
config = utils.load_config(args.config)

path = utils.get_run_dir(config)

turn_on_network_measure = utils.is_network_measure(config)

server_cmd = "./carousel-server "
client_cmd = "./client "
check_server_status_cmd = utils.binPath + "/checkServerStatus "
enforce_leader_cmd = utils.binPath + "/enforce-leader "
network_measure_cmd = "./networkMeasure "

if args.debug:
    server_cmd = server_cmd + "-d "
    client_cmd = client_cmd + "-d "
    check_server_status_cmd = check_server_status_cmd + "-d "
    enforce_leader_cmd = enforce_leader_cmd + "-d "
    network_measure_cmd += "-d "

machines_client = utils.parse_client_machine(config)

machines_server = utils.parse_server_machine(config)

machines_network_measure = utils.parse_network_measure_machine(config)


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
        thread = threading.Thread(target=scp_client_log_exec, args=(new_dir, machine.ssh_client, machine.scp_client, ip))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return new_dir


def scp_networkMeasure_log_exec(new_dir, ssh, scp, ip):
    client_dir = config["experiment"]["runDir"] + "/networkMeasure"
    stdin, stdout, stderr = ssh.exec_command("ls " + client_dir + "/*.log")
    log_files = stdout.read().split()
    for log in log_files:
        scp.get(log, new_dir)
    ssh.exec_command("rm " + client_dir + "/*.log")
    print("collect network measure log from " + ip)


def collect_networkMeasure_log(new_dir):
    threads = list()
    for ip, machine in machines_network_measure.items():
        if len(machine.ids) == 0:
            continue
        thread = threading.Thread(target=scp_networkMeasure_log_exec, args=(new_dir, machine.ssh_client, machine.scp_client, ip))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


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
        for sId in machine.ids:
            thread = threading.Thread(target=scp_server_log_exec, args=(new_dir, machine.ssh_client, machine.scp_client, sId, ip))
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
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        # cmd += "cd " + path + ";"
        exe = "cd " + path + "/server-$id; " + \
              server_cmd + "-i $id" + " -c " + args.config + " > " + "server-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done"
        cmd += loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.ssh_client, cmd, ip, machine.ids))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def start_network_measure():
    threads = list()
    for ip, machine in machines_network_measure.items():
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        exe = "cd " + path + "/networkMeasure; " + \
              network_measure_cmd + "-i $id -c " + args.config + " > " + "networkMeasure-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done;"
        cmd += loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.ssh_client, cmd, ip))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def start_clients():
    threads = list()

    start_time = str(int((time.time() + 10) * 1000 * 1000 * 1000))
    for ip, machine in machines_client.items():
        if len(machine.ids) == 0:
            continue
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        # cmd += "cd " + path + "; mkdir -p client;" + " cp " + path + "/" + args.config + " " + path + "/client/; "
        exe = "cd " + path + "/client;" + \
              client_cmd + "-i $id" + " -c " + args.config + " -t " + start_time + " > " + "client-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done; wait"
        cmd += loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.ssh_client, cmd, ip))
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
        server_dir = path + "/server-$id"
        exe = "cd " + server_dir + "; " + "rm -r raft-*; rm -r *.log;"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done"
        cmd = "killall -9 carousel-server; " + loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.ssh_client, cmd, ip, ip, True))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def stop_network_measure():
    threads = list()
    for ip, machine in machines_network_measure.items():
        cmd = "killall -9 networkMeasure"
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.ssh_client, cmd, ip, ip, True))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def stop_clients():
    threads = list()
    for ip, machine in machines_client.items():
        cmd = "killall -9 client"
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.ssh_client, cmd, ip, ip, True))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def main():
    start_time = time.time()
    start_servers()
    time.sleep(15)
    end_start_server = time.time()
    start_server_use = end_start_server - start_time
    print("start server use (+15s) %.5fs" % start_server_use)
    end_start_network_measure = time.time()
    if turn_on_network_measure:
        start_network_measure()
        time.sleep(10)
        end_start_network_measure = time.time()
        start_network_measure_use = end_start_network_measure - end_start_server
        print("start network measure used %.5fs" % start_network_measure_use)
    start_client_time = datetime.datetime.now().strftime("%H:%M:%S")
    print("start client at time " + start_client_time)
    start_clients()
    end_client = time.time()
    client_use = end_client - end_start_network_measure
    print("clients finish used %.5fs" % client_use)
    dir_name = collect_client_log()
    if args.debug:
        if turn_on_network_measure:
            collect_networkMeasure_log(dir_name)
        print_server_status(dir_name)
        end_server = time.time()
        server_use = end_server - end_client
        print("server finish used %.5fs" % server_use)
        collect_server_log(dir_name)
    end_collect = time.time()
    collect_use = end_collect - end_client
    print("collect log used %.5fs" % collect_use)
    if turn_on_network_measure:
        stop_network_measure()
    stop_servers()
    end_time = time.time()
    stop_server_use = end_time - end_collect
    print("stop client and server use %.5f" % stop_server_use)
    print("-----")
    print("entire exp use %.5fs" % (end_time - start_time))
    print("start server use (+15s) %.5fs" % start_server_use)
    print("run clients used %.5fs" % client_use)
    # print("server finish used %.5fs" % server_use)
    # print("collect log used %.5fs" % collect_use)
    print("stop client and server use %.5f" % stop_server_use)


if __name__ == "__main__":
    main()
