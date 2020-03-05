#!/usr/bin/python
import subprocess
import os
import argparse

path = "/home/l69yang/Projects/go/src/Carousel-GTS/exp"

arg_parser = argparse.ArgumentParser(description="run exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=False)

args = arg_parser.parse_args()


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
    subprocess.call(["../sbin/run.py", "-c", f])
    dir_name = f.split('.')[0] + "-" + str(i)
    move_log(dir_name)


def remove_log(dir_path):
    lists = os.listdir(dir_path)
    for f in lists:
        if f.endswith(".log"):
            os.remove(os.path.join(dir_path, f))


def move_log(dir_name):
    lists = os.listdir(path)
    new_dir = os.path.join(path, dir_name)
    if os.path.isdir(new_dir):
        remove_log(new_dir)
    else:
        os.mkdir(new_dir)
    for f in lists:
        if f.endswith(".log"):
            os.rename(os.path.join(path, f), os.path.join(new_dir, f))


def main():
    for i in range(1):
        run_exp(i)


if __name__ == "__main__":
    main()
