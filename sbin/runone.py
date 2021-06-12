#!/usr/bin/python
import datetime
import time
import threading
import os
import subprocess
import utils


def scp_client_log_exec(new_dir, ssh, scp, ip, client_dir):
    # client_dir = config["experiment"]["runDir"] + "/client"
    stdin, stdout, stderr = ssh.exec_command("ls " + client_dir + "/*.log " + client_dir + "/*.statistic")
    #stdin, stdout, stderr = ssh.exec_command("ls " + client_dir + "/*.statistic")
    log_files = stdout.read().split()
    for log in log_files:
        scp.get(log, new_dir)
    ssh.exec_command("rm " + client_dir + "/*.log")
    ssh.exec_command("rm " + client_dir + "/*.statistic")
    print("collect client log from " + ip)


def collect_client_log(machines_client, run_count, config_file_name, run_dir):
    threads = list()
    dir_name = config_file_name.split('.')[0] + "-" + str(run_count)
    new_dir = os.path.join(os.getcwd(), dir_name)
    if os.path.isdir(new_dir):
        subprocess.call("rm -r " + new_dir + "/*", shell=True)
    else:
        os.mkdir(new_dir)

    client_dir = run_dir + "/client"
    for ip, machine in machines_client.items():
        if len(machine.ids) == 0:
            continue
        thread = threading.Thread(
            target=scp_client_log_exec,
            args=(new_dir, machine.get_ssh_client(), machine.get_scp_client(), ip, client_dir))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return new_dir


def scp_networkMeasure_log_exec(new_dir, ssh, scp, ip, network_measure_dir):
    # client_dir = config["experiment"]["runDir"] + "/networkMeasure"
    stdin, stdout, stderr = ssh.exec_command("ls " + network_measure_dir + "/*.log")
    log_files = stdout.read().split()
    for log in log_files:
        scp.get(log, new_dir)
    ssh.exec_command("rm " + network_measure_dir + "/*.log")
    print("collect network measure log from " + ip)


def collect_networkMeasure_log(new_dir, machines_network_measure, run_dir):
    network_measure_dir = run_dir + "/networkMeasure"
    threads = list()
    for ip, machine in machines_network_measure.items():
        if len(machine.ids) == 0:
            continue
        thread = threading.Thread(
            target=scp_networkMeasure_log_exec,
            args=(new_dir, machine.get_ssh_client(), machine.get_scp_client(), ip, network_measure_dir))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def scp_server_log_exec(new_dir, ssh, scp, run_dir, ids, ip):
    for s_id in ids:
        server_dir = run_dir + "/server-" + str(s_id)
        stdin, stdout, stderr = ssh.exec_command("ls " + server_dir + "/*.log")
        log_files = stdout.read().split()
        for log in log_files:
            scp.get(log, new_dir)
        ssh.exec_command("rm -r " + server_dir + "/raft-*")
        ssh.exec_command("rm -r " + server_dir + "/*.log")
        print("collect server log " + server_dir + " from " + ip)


def collect_server_log(new_dir, machines_server, run_dir):
    threads = list()
    for ip, machine in machines_server.items():
        if len(machine.ids) == 0:
            continue
        thread = threading.Thread(
                target=scp_server_log_exec,
                args=(new_dir, machine.get_ssh_client(), machine.get_scp_client(), run_dir, machine.ids, ip))
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


def start_servers(machines_server, debug, config_file_name, run_dir, cpuProfile):
    threads = list()
    server_cmd = utils.get_server_cmd(debug)
    if cpuProfile:
        server_cmd += "-cpuprofile server-$id-cpu.log "
    for ip, machine in machines_server.items():
        if len(machine.ids) == 0:
            continue
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        exe = "cd " + run_dir + "/server-$id; " + \
              server_cmd + "-i $id" + " -c " + config_file_name + " > " + "server-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done"
        cmd += loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.get_ssh_client(), cmd, ip, machine.ids))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def start_network_measure(machines_network_measure, debug, config_file_name, run_dir):
    threads = list()
    network_measure_cmd = utils.get_network_measure_cmd(debug)
    for ip, machine in machines_network_measure.items():
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        exe = "cd " + run_dir + "/networkMeasure; " + \
              network_measure_cmd + "-i $id -c " + config_file_name + " > " + "networkMeasure-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done;"
        cmd += loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.get_ssh_client(), cmd, ip))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def start_clients(machines_client, debug, config_file_name, run_dir):
    threads = list()
    client_cmd = utils.get_client_cmd(debug)
    start_time = str(int((time.time() + 3) * 1000 * 1000 * 1000))
    for ip, machine in machines_client.items():
        if len(machine.ids) == 0:
            continue
        cmd = "ulimit -c unlimited;"
        cmd += "ulimit -n 100000;"
        # cmd += "cd " + path + "; mkdir -p client;" + " cp " + path + "/" + args.config + " " + path + "/client/; "
        exe = "cd " + run_dir + "/client;" + \
              client_cmd + "-i $id" + " -c " + config_file_name + " -t " + start_time + " > " + "client-$id.log " + "2>&1 &"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done; wait"
        cmd += loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.get_ssh_client(), cmd, ip))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def print_server_status(dir_name, debug, config_file_name):
    cmd = utils.get_check_server_status_cmd(debug) + "-c " + config_file_name + " -r " + dir_name
    subprocess.call(cmd, shell=True)


# def enforce_leader():
#     if config["servers"]["replicationFactor"] > 1:
#         cmd = enforce_leader_cmd + "-c " + args.config
#         subprocess.call(cmd, shell=True)


def stop_servers(machines_server, run_dir):
    threads = list()
    for ip, machine in machines_server.items():
        server_dir = run_dir + "/server-$id"
        #exe = "cd " + server_dir + "; " + "rm -r raft-*; rm -r *.log;"
        exe = "cd " + server_dir + "; " + "rm -r raft-*;"
        loop = "for id in " + ' '.join(machine.ids) + "; do " + exe + " done"
        cmd = "killall -9 carousel-server; " + loop
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.get_ssh_client(), cmd, ip, ip, True))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def stop_network_measure(machines_network_measure):
    threads = list()
    for ip, machine in machines_network_measure.items():
        cmd = "killall -9 networkMeasure"
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.get_ssh_client(), cmd, ip, ip, True))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def stop_clients(machines_client):
    threads = list()
    for ip, machine in machines_client.items():
        cmd = "killall -9 client"
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread, args=(machine.get_ssh_client(), cmd, ip, ip, True))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def run_config(config_file_name, debug, i, cpuProfile, machines_client, machines_server, machines_network_measure):
    config = utils.load_config(config_file_name)
    run_dir = utils.get_run_dir(config)
    turn_on_network_measure = utils.is_network_measure(config)
    #stop_servers(machines_server, run_dir)
    start_time = time.time()
    start_servers(machines_server, debug, config_file_name, run_dir, cpuProfile)
    time.sleep(15)
    end_start_server = time.time()
    start_server_use = end_start_server - start_time
    print("start server use (+15s) %.5fs" % start_server_use)
    end_start_network_measure = time.time()
    if utils.is_network_measure(config):
        start_network_measure(machines_network_measure, debug, config_file_name, run_dir)
        time.sleep(5)
        end_start_network_measure = time.time()
        start_network_measure_use = end_start_network_measure - end_start_server
        print("start network measure used %.5fs" % start_network_measure_use)
    start_client_time = datetime.datetime.now().strftime("%H:%M:%S")
    print("start client at time " + start_client_time)
    start_clients(machines_client, debug, config_file_name, run_dir)
    end_client = time.time()
    client_use = end_client - end_start_network_measure
    print("clients finish used %.5fs" % client_use)
    dir_name = collect_client_log(machines_client, i, config_file_name, run_dir)
    if debug:
        if turn_on_network_measure:
            collect_networkMeasure_log(dir_name, machines_network_measure, run_dir)
        print_server_status(dir_name, debug, config_file_name)
        end_server = time.time()
        server_use = end_server - end_client
        print("server finish used %.5fs" % server_use)
        collect_server_log(dir_name, machines_server, run_dir)
    end_collect = time.time()
    collect_use = end_collect - end_client
    print("collect log used %.5fs" % collect_use)
    if turn_on_network_measure:
        stop_network_measure(machines_network_measure)
    #print_server_status(dir_name, debug, config_file_name)
    stop_servers(machines_server, run_dir)
    #collect_server_log(dir_name, machines_server, run_dir)
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


# if __name__ == "__main__":
#     main()
