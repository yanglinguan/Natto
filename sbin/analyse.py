#!/usr/bin/python
import os
import argparse
import sys
import numpy

arg_parser = argparse.ArgumentParser(description="analyse.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=False)
args = arg_parser.parse_args()

path = os.getcwd()
if args.config is not None:
    path = args.config

low = 15 * 1000000000
high = 75 * 1000000000


def analyse_waiting():
    txn_map = {}
    lists = os.listdir(path)
    for f in lists:
        if f.endswith("_commitOrder.log"):
            lines = open(os.path.join(path, f), "r").readlines()
            for line in lines:
                items = line.split(" ")
                txn_id = items[0]
                wait_num = int(items[1])
                if txn_id not in txn_map:
                    txn_map[txn_id] = wait_num
                txn_map[txn_id] = max(txn_map[txn_id], wait_num)

    f = open("waiting.analyse", "w")
    for key, value in txn_map.items():
        f.write(key + " " + str(value) + "\n")


def load_statistic():
    txn_map = {}
    lists = os.listdir(path)
    min_start = sys.maxsize
    for f in lists:
        if f.endswith("_statistic.log"):
            lines = open(os.path.join(path, f), "r").readlines()
            for line in lines:
                if line.startswith("#"):
                    continue
                items = line.split(",")
                txn_id = items[0]
                commit = bool(int(items[1]))
                latency = float(items[2]) / 1000000  # ms
                start = float(items[3])
                end = float(items[4])
                if start < min_start:
                    min_start = start
                txn_map[txn_id] = {"commit": commit, "latency": latency, "start": start, "end": end}

    for txn_id, value in txn_map.items():
        value["start"] = value["start"] - min_start
        if value["start"] < low or value["start"] > high:
            del txn_map[txn_id]

    return txn_map


def analyse_latency(txn_map):
    latency = []
    for txn_id, value in txn_map.items():
        if value["commit"]:
            latency.append(value["latency"])

    median = numpy.percentile(latency, 50)
    p95 = numpy.percentile(latency, 95)
    p99 = numpy.percentile(latency, 99)
    p10 = numpy.percentile(latency, 10)
    avg = numpy.average(latency)

    print("10 per (ms): " + str(p10))
    print("median (ms): " + str(median))
    print("95 per (ms): " + str(p95))
    print("99 per (ms): " + str(p99))
    print("avg (ms): " + str(avg))


def analyse_throughput(txn_map):
    min_time = sys.maxsize
    max_time = -sys.maxsize - 1
    count = 0
    for txn_id, value in txn_map.items():
        if value["start"] < min_time:
            min_time = value["start"]

        if value["start"] > max_time:
            max_time = value["start"]

        count += 1

    throughput = float(count * 1000000000) / (max_time - min_time)
    print("commit throughput (txn/s): " + str(throughput))


def analyse_abort_rate(txn_map):
    commit = 0
    abort = 0
    for txn_id, value in txn_map.items():
        if value["commit"]:
            commit += 1
        else:
            abort += 1

    commit_rate = float(commit) / (abort + commit)
    print("Commit rate: " + str(commit_rate))


def main():
    # analyse_waiting()
    txn_map = load_statistic()
    analyse_latency(txn_map)
    analyse_throughput(txn_map)
    analyse_abort_rate(txn_map)


if __name__ == "__main__":
    main()
