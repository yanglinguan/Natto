#!/usr/bin/python
import json
import os
import argparse
import sys
import numpy

arg_parser = argparse.ArgumentParser(description="analyse.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=False)
args = arg_parser.parse_args()

# path = os.getcwd()
# if args.config is not None:
#     path = args.config

low = 0 * 1000000000
high = 90 * 1000000000


# low = 15 * 1000000000
# high = 75 * 1000000000


def analyse_waiting(dir_name):
    path = dir_name
    txn_map = {}
    lists = os.listdir(path)
    for f in lists:
        if f.endswith("_commitOrder.log"):
            lines = open(os.path.join(path, f), "r").readlines()
            for line in lines:
                items = line.split(" ")
                txn_id = items[0]
                wait_num = int(items[1])
                wait_num_t = 0
                x = 0
                y = 0
                z = 0
                t = 0
                if len(items) == 7:
                    wait_num_t = int(items[2])
                    x = float(items[3]) / 1000000
                    y = float(items[4]) / 1000000
                    z = float(items[5]) / 1000000
                    t = int(items[6])
                if txn_id not in txn_map:
                    txn_map[txn_id] = [wait_num, wait_num_t, x, y, z, t]
                txn_map[txn_id][0] = max(txn_map[txn_id][0], wait_num)
                txn_map[txn_id][1] = max(txn_map[txn_id][1], wait_num_t)
                txn_map[txn_id][2] = max(txn_map[txn_id][2], x)
                txn_map[txn_id][3] = max(txn_map[txn_id][3], y)
                txn_map[txn_id][4] = max(txn_map[txn_id][4], z)
                txn_map[txn_id][5] = txn_map[txn_id][5] + t

    f = open("waiting.analyse", "w")
    for key, value in txn_map.items():
        s = key
        for x in value:
            s = s + " " + str(x)
        s += "\n"
        f.write(s)
    f.close()


def load_statistic(dir_name):
    path = dir_name
    txn_map = {}
    lists = os.listdir(path)
    min_start = sys.maxsize
    for f in lists:
        if f.endswith(".statistic"):
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
    p90 = numpy.percentile(latency, 90)
    p95 = numpy.percentile(latency, 95)
    p99 = numpy.percentile(latency, 99)
    p10 = numpy.percentile(latency, 10)
    avg = numpy.average(latency)

    result = {"median": median, "p90": p90, "p95": p95, "p10": p10, "p99": p99, "avg": avg}

    print("10 per (ms): " + str(p10))
    print("median (ms): " + str(median))
    print("90 per (ms): " + str(p90))
    print("95 per (ms): " + str(p95))
    print("99 per (ms): " + str(p99))
    print("avg (ms): " + str(avg))

    return result


def analyse_throughput(txn_map):
    min_time = sys.maxsize
    max_time = -sys.maxsize - 1
    count = 0
    for txn_id, value in txn_map.items():
        if value["start"] < min_time:
            min_time = value["start"]

        if value["start"] > max_time:
            max_time = value["start"]
        if value["commit"]:
            count += 1

    throughput = float(count * 1000000000) / (max_time - min_time)
    print("start time " + str(min_time) + "; end time" + str(max_time))
    print("commit throughput (txn/s): " + str(throughput))
    return throughput


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
    return commit_rate


def analyse(dir_name):
    path = dir_name
    analyse_waiting(path)
    txn_map = load_statistic(path)
    result = analyse_latency(txn_map)
    throughput = analyse_throughput(txn_map)
    commit_rate = analyse_abort_rate(txn_map)

    result["throughput"] = throughput
    result["commit_rate"] = commit_rate

    file_name = os.path.basename(path)
    with open(file_name + ".result", "w") as f:
        json.dump(result, f, indent=4)


def main():
    if args.config is not None:
        analyse(args.config)
    else:
        path = os.getcwd()
        lists = os.listdir(path)
        for f in lists:
            if os.path.isdir(os.path.join(path, f)):
                analyse(f)


if __name__ == "__main__":
    main()
