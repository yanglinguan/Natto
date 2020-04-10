#!/usr/bin/python
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

args = arg_parser.parse_args()

bin_path = "/home/l69yang/Projects/go/src/Carousel-GTS/sbin/"

timeout = 6 * 60


def run_exp(i):
    run_list = []
    if args.config is not None:
        run_list.append(args.config)
    else:
        lists = os.listdir(path)
        for f in lists:
            if f.endswith(".json"):
                run_list.append(f)

    for f in run_list:
        p = multiprocessing.Process(target=run, name="run", args=(i, f))
        p.start()
        p.join(timeout)
        if p.is_alive():
            print("config " + f + " is still running after " + str(timeout/60) + "min, kill it")
            subprocess.call([bin_path + "stop.py", "-c", f])
            notification("Warning: " + f + " is running over 10 min. Experiment terminated")
            p.terminate()
            p.join()


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


def main():
    for i in range(1):
        run_exp(i)

    notification("experiment is finish")


if __name__ == "__main__":
    main()
