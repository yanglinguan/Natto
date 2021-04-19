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
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
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

args = arg_parser.parse_args()

bin_path = "/home/l69yang/Projects/go/src/Carousel-GTS/sbin/"

timeout = 10 * 60
n = 1
if args.num is not None:
    n = int(args.num)


def getNextRunCount(f):
    prefix = f.split(".")[0]
    i = 0
    runN = prefix + "-" + str(i)
    while os.path.exists(runN):
        i += 1
        runN = prefix + "-" + str(i)
    return i


def getRunExpNum(f):
    prefix = f.split(".")[0]
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
        p = multiprocessing.Process(target=run, name="run", args=(nextRun, f))
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


def run(i, f):
    # print("run " + f + " " + str(i))
    if args.debug:
        subprocess.call([bin_path + "run.py", "-d", "-c", f, "-r", str(i)])
    else:
        subprocess.call([bin_path + "run.py", "-c", f, "-r", str(i)])


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


def main():
    finishes = 0

    if args.machines is not None and args.directory is not None:
        subprocess.call([bin_path + "gen_config.py", "-m", args.machines, "-d", args.directory])
        subprocess.call(["cd", args.directory])

    run_list = []
    if args.config is not None:
        idx = getRunIdx(args.config)
        if idx < 0:
            print(args.config, " already run " + str(n) + " times. use -f force to run " + str(n) + " times")
            return
        run_list.append((args.config, idx))
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
    for i in range(n):
        finish, succ, failed = run_exp(i, run_list)
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
