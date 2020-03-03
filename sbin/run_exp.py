#!/usr/bin/python
import subprocess
import os

path = "/home/l69yang/Projects/go/src/Carousel-GTS/exp"


def run_exp(i):
    lists = os.listdir(path)
    for f in lists:
        if f.endswith(".json"):
            subprocess.call(["../sbin/run.py", "-c", f])
            dir_name = f.split('.')[0] + "-" + str(i)
            move_log(dir_name)


def move_log(dir_name):
    lists = os.listdir(path)
    new_dir = os.path.join(path, dir_name)
    for f in lists:
        if f.endswith(".log"):
            os.rename(os.path.join(path, f), os.path.join(new_dir, f))


def main():
    for i in range(1):
        run_exp(i)


if __name__ == "__main__":
    main()
