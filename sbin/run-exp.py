#!/usr/bin/python
import datetime
import multiprocessing
import smtplib
import subprocess
import os
import argparse

path = os.getcwd()

arg_parser = argparse.ArgumentParser(description="run exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='*',
                        help='configuration file', required=False)
arg_parser.add_argument('-d', '--debug', help="turn on debug",
                        action='store_true')
arg_parser.add_argument('-e', '--senderEmail', dest="senderEmail", nargs='?',
                        help='sender email address', required=False)
arg_parser.add_argument('-p', '--password', dest="password", nargs='?',
                        help='sender email password', required=False)
arg_parser.add_argument('-r', '--receiverEmail', dest="receiverEmail", nargs='?',
                        help='receive email address', required=False)

arg_parser.add_argument('-m', '--machines', dest="machines", nargs='?',
                        help='machines config file', required=False)
arg_parser.add_argument('-dir', '--directory', dest="directory", nargs='?',
                        help='config file directory', required=False)
arg_parser.add_argument('-n', '--num', dest="num", nargs='?',
                        help='number of run', required=False)

arg_parser.add_argument('-f', '--force',
                        help="force exp to run n times", action='store_true')
arg_parser.add_argument('-v', '--variance',
                        help="network variance", action='store_true')

args = arg_parser.parse_args()

bin_path = "/home/l69yang/Projects/go/src/Carousel-GTS/sbin/"

timeout = 300 * 60
n = 1
if args.num is not None:
    n = int(args.num)


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


def run_exp(i, run_list):
    finishes = 0
    errorRun = []
    for item in run_list:
        f = item[0]
        x = item[1]
        if i < x:
            print(f + " already run " + str(x) + " times skip this time " + str(i))
            continue
        nextRun = getNextRunCount(f)
        p = multiprocessing.Process(target=run, name="run", args=(nextRun, f, i == x))
        p.start()
        p.join(timeout)
        if p.is_alive():
            print("config " + f + " is still running after " + str(timeout / 60) + " min, kill it at " +
                  datetime.datetime.now().strftime("%H:%M:%S"))
            subprocess.call([bin_path + "stop.py", "-c", f])
            # p.terminate()
            p.join()
            errorRun.append(f.split(".")[0] + "-" + str(i))
            # return finishes, False, f
        finishes = finishes + 1
    return finishes, True, errorRun


def run(i, f, b):
    # print("run " + f + " " + str(i))
    run_args = [bin_path + "run.py", "-c", f, "-r", str(i)]
    if args.debug:
        run_args.append("-d")
    if b:
        run_args.append("-b")
    subprocess.call(run_args)


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
    exp_list = ["client_nums", "workload_highPriority", "zipfAlpha", "txnSize", "txnRate"]
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
    finishes = 0

    if args.machines is not None and args.directory is not None:
        subprocess.call([bin_path + "gen_config.py", "-m", args.machines, "-d", args.directory])
        subprocess.call(["cd", args.directory])

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
        for i in range(n):
            finish, succ, failed = run_exp(i, rlist)
            if succ:
                finishes += finish
            else:
                for f in failed:
                    errorRun.append(f)
                error = ",".join(failed)
                notification("need to rerun config " + error + " exp failed")
                # return

    # if finishes > 1 or len(errorRun) > 0:
    error = ",".join(errorRun)
    notification("experiment is finish. need to rerun config " + error)


if __name__ == "__main__":
    main()
