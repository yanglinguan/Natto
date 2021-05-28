#!/usr/bin/python
import datetime
# import multiprocessing
import smtplib
import subprocess
import os
import argparse
import threading
import time

import utils
import runone

path = os.getcwd()

arg_parser = argparse.ArgumentParser(description="run exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='*',
                        help='configuration file', required=False)
arg_parser.add_argument('-d', '--debug', help="turn on debug",
                        action='store_true')
arg_parser.add_argument('-n', '--num', dest="num", nargs='?',
                        help='number of run', required=False)
arg_parser.add_argument('-f', '--force',
                        help="force exp to run n times", action='store_true')
arg_parser.add_argument('-v', '--variance',
                        help="network variance", action='store_true')
arg_parser.add_argument('-N', '--noBuildDeploy', help="run without build and deploy",
                        action='store_true')
arg_parser.add_argument('-cp', '--cpuProfile', help="turn on cpu profiling",
                        action='store_true')

# for email notification
arg_parser.add_argument('-e', '--senderEmail', dest="senderEmail", nargs='?',
                        help='sender email address', required=False)
arg_parser.add_argument('-p', '--password', dest="password", nargs='?',
                        help='sender email password', required=False)
arg_parser.add_argument('-r', '--receiverEmail', dest="receiverEmail", nargs='?',
                        help='receive email address', required=False)

args = arg_parser.parse_args()

bin_path = "/home/l69yang/Projects/go/src/Carousel-GTS/sbin/"

timeout = 300 * 60
n = 1
if args.num is not None:
    n = int(args.num)


def build():
    try:
        print("build server at " + utils.server_path)
        subprocess.call("cd " + utils.rpc_path + "; protoc --go_out=plugins=grpc:. *.proto", shell=True)
        subprocess.call("cd " + utils.server_path + "; go install", shell=True)
        print("build client at " + utils.client_path)
        subprocess.call("cd " + utils.client_path + "; go install", shell=True)
        print("build tool check server status at " + utils.check_server_status_path)
        subprocess.call("cd " + utils.check_server_status_path + "; go install", shell=True)
        print("build network measure at " + utils.network_measure_path)
        subprocess.call("cd " + utils.network_measure_path + "; go install", shell=True)
        subprocess.call("cd " + os.getcwd(), shell=True)
    except subprocess.CalledProcessError:
        print("build error")


def scp_exec(ssh, scp, ip, run_dir, scp_files, ids=[]):
    if len(ids) == 0:
        ssh.exec_command("mkdir -p " + run_dir)
        scp.put(scp_files, run_dir)
        print("finish deploy config and server ", scp_files, " at ", ip, run_dir)
        return
    for sId in ids:
        server_dir = run_dir + "/server-" + str(sId)
        ssh.exec_command("mkdir -p " + server_dir)
        scp.put(scp_files, server_dir)
        print("finish deploy config and server ", scp_files, " at ", ip, server_dir)


def deploy_servers(run_list, threads, run_dir, machines_server):
    server_scp_files = [os.getcwd() + "/" + f for f in run_list]
    server_scp_files.append(utils.binPath + "carousel-server")
    for ip, machine in machines_server.items():
        thread = threading.Thread(target=scp_exec,
                                args=(machine.get_ssh_client(), machine.get_scp_client(), ip, run_dir, server_scp_files, machine.ids))
        threads.append(thread)
        thread.start()
    print("deploy servers threads started")


def deploy_client(run_list, threads, run_dir, machines_client):
    client_scp_files = [os.getcwd() + "/" + f for f in run_list]
    client_scp_files.append(utils.binPath + "client")
    client_dir = run_dir + "/client"
    for ip, machine in machines_client.items():
        if len(machine.ids) == 0:
            continue
        thread = threading.Thread(target=scp_exec,
                                  args=(machine.get_ssh_client(), machine.get_scp_client(), ip, client_dir, client_scp_files))
        threads.append(thread)
        thread.start()
    print("deploy clients threads started")


def deploy_network_measure(run_list, threads, run_dir, machines_network_measure):
    network_measure_scp_files = [os.getcwd() + "/" + f for f in run_list]
    network_measure_scp_files.append(utils.binPath + "networkMeasure")
    network_measure_dir = run_dir + "/networkMeasure"
    for ip, machine in machines_network_measure.items():
        thread = threading.Thread(target=scp_exec, args=(
            machine.get_ssh_client(), machine.get_scp_client(), ip, network_measure_dir, network_measure_scp_files))
        threads.append(thread)
        thread.start()
    print("deploy network measure threads started")


def deploy(run_list, run_dir, machines_client, machines_server, machines_network_measure):
    threads = []
    deploy_servers(run_list, threads, run_dir, machines_server)
    deploy_client(run_list, threads, run_dir, machines_client)
    deploy_network_measure(run_list, threads, run_dir, machines_network_measure)

    for thread in threads:
        thread.join()


def set_network_delay(f):
    subprocess.call([bin_path + "delay-set.py", "-c", f])


def getNextRunCount(f):
    prefix = f.split(".")[0]
    i = 0
    runN = prefix + "-" + str(i)
    while os.path.exists(runN):
        i += 1
        runN = prefix + "-" + str(i)
    return i


def getRunExpNum(f):
    prefix = f.split(".")[0] + "-"
    i = 0
    for d in os.listdir(path):
        if os.path.isdir(d) and d.startswith(prefix):
            i += 1
    return i


def run_exp(i, run_conf, machines_client, machines_server, machines_network_measure):
    finishes = 0
    errorRun = []
    #for item in run_list:
    f = run_conf[0]
    x = run_conf[1]
    if i < x:
        print(f + " already run " + str(x) + " times skip this time " + str(i))
        return
    nextRun = getNextRunCount(f)
    thread = threading.Thread(
        target=runone.run_config,
        args=(f, args.debug, nextRun, args.cpuProfile, machines_client, machines_server, machines_network_measure))
    thread.start()
    thread.join(timeout)
    # p = multiprocessing.Process(
    #     target=runone.run_config,
    #     args=(f, args.debug, nextRun, machines_client, machines_server, machines_network_measure))
    # p.start()
    # p.join(timeout)
    if thread.is_alive():
        print("config " + f + " is still running after " + str(timeout / 60) + " min, kill it at " +
              datetime.datetime.now().strftime("%H:%M:%S"))
        subprocess.call([bin_path + "stop.py", "-c", f])
        # p.terminate()
        thread.join()
        errorRun.append(f.split(".")[0] + "-" + str(i))
        # return finishes, False, f
    finishes = finishes + 1
    return finishes, True, errorRun


# def run(i, f, machines_client, machines_server, machines_network_measure):
#     # config_file_name, debug, i, machines_client, machines_server, machines_network_measure
#     # print("run " + f + " " + str(i))
#     runone.run_config(f, args.debug, i, machines_client, machines_server, machines_network_measure)
#     # run_args = [bin_path + "runone.py", "-c", f, "-r", str(i)]
#     # if args.debug:
#     #     run_args.append("-d")
#     # subprocess.call(run_args)


def remove_log(dir_path):
    lists = os.listdir(dir_path)
    for f in lists:
        if f.endswith(".log"):
            os.remove(os.path.join(dir_path, f))


def notification(message):
    if args.senderEmail is None or args.password is None or args.receiverEmail is None:
        return
    mail = smtplib.SMTP('smtp.gmail.com', 587)
    mail.ehlo()
    mail.starttls()
    mail.login(args.senderEmail, args.password)
    mail.sendmail(args.senderEmail, args.receiverEmail, message)
    mail.close()


def getRunIdx(f):
    if args.force:
        return 0
    exp_run_num = getRunExpNum(f)
    x = n - exp_run_num
    if x <= 0:
        return -1
    return exp_run_num


def getSortkey(f):
    key = (f[0].split(".")[0]).split("-")[-1]
    return int(key)


def sort_run_list(run_list):
    exp_list = ["txnRate","zipfAlpha", "workload_highPriority"]
    result_map = {}
    not_found = []
    for rc in run_list:
        f = rc[0].split("-")[1]
        found = False
        for e in exp_list:
            if f.startswith(e):
                if e not in result_map:
                    result_map[e] = []
                result_map[e].append(rc)
                found = True
                break
        if not found:
            not_found.append(rc)
    result_list = []
    for e in exp_list:
        if e in result_map:
            result_map[e].sort(key=getSortkey)
            result_list.append(result_map[e])
    if len(not_found) != 0:
        result_list.append(not_found)
    return result_list


def sort_variance_run_list(run_list):
    run_list.sort(key=getSortkey)
    result_list = []
    for i in range(len(run_list)):
        if i == 0 or getSortkey(run_list[i]) != getSortkey(run_list[i - 1]):
            result_list.append([run_list[i]])
            continue
        result_list[-1].append(run_list[i])
    return result_list


def main():
    start = time.time()
    finishes = 0

    run_list = []
    if args.config is not None:
        for c in args.config:
            if not c.endswith(".json"):
                continue
            idx = getRunIdx(c)
            if idx < 0:
                print(c, " already run " + str(n) + " times. use -f force to run " + str(n) + " times")
                continue
            run_list.append((c, idx))
    else:
        lists = os.listdir(path)
        for f in lists:
            if f.endswith(".json"):
                idx = getRunIdx(f)
                if idx < 0:
                    print(f + " already run " + str(n) + " times. use -f force to run " + str(n) + " times")
                    continue
                # (fname, num of exp already run)
                print(f + " needs to run " + str(n - idx) + " times. Already run " + str(idx) + " times")
                run_list.append((f, idx))

    if len(run_list) == 0:
        return

    config = utils.load_config(run_list[0][0])
    run_dir = utils.get_run_dir(config)
    machines_client = utils.parse_client_machine(config)
    machines_network_measure = utils.parse_network_measure_machine(config)
    machines_server = utils.parse_server_machine(config)

    utils.create_ssh_client([machines_client, machines_server, machines_network_measure])

    if not args.noBuildDeploy:
        start_build = time.time()
        build()
        end_build = time.time()
        build_used = end_build - start_build
        print("build used %.5fs" % build_used)
        start_deploy = time.time()
        deploy([rl[0] for rl in run_list], run_dir, machines_client, machines_server, machines_network_measure)
        end_deploy = time.time()
        deploy_used = end_deploy - start_deploy
        print("deploy used %.5fs" % deploy_used)
    errorRun = []
    if args.variance:
        run_list = sort_variance_run_list(run_list)
    else:
        run_list = sort_run_list(run_list)

    for rlist in run_list:
        if len(rlist) == 0:
            continue
        if args.variance:
            print("set varince", rlist[0][0])
            set_network_delay(rlist[0][0])
        print(rlist)
        for run_conf in rlist:
            for i in range(n):
                run_exp(i, run_conf, machines_client, machines_server, machines_network_measure)
                #if succ:
                #    finishes += finish
                #else:
                #    for f in failed:
                #        errorRun.append(f)
                #    error = ",".join(failed)
                #    notification("need to rerun config " + error + " exp failed")
                    # return

    # if finishes > 1 or len(errorRun) > 0:
    error = ",".join(errorRun)
    notification("experiment is finish. need to rerun config " + error)
    end = time.time()
    print("Total exp time %.5fs" % (end - start))


if __name__ == "__main__":
    main()
