#!/usr/bin/python3
import itertools
import json
import os
import argparse
import sys
import shutil
import numpy
import math

arg_parser = argparse.ArgumentParser(description="analyse.")

# Cluster configuration file
arg_parser.add_argument('-d', '--resultDir', dest='resultDir', nargs='*',
                        help='result dir', required=False)
arg_parser.add_argument('-c', '--configFile', dest='configFile', nargs='*',
                        help='configuration file', required=False)
arg_parser.add_argument('-f', '--force', dest='force',
                        help="force to analyse", action='store_true', required=False)
arg_parser.add_argument('-nr', '--noReadOnly', dest="noReadOnly", required=False,
                        help="no include the read only txn", action='store_true')
arg_parser.add_argument('-inc', '--increase', dest="increase", action='store_true', required=False)

args = arg_parser.parse_args()

# path = os.getcwd()
# if args.config is not None:
#     path = args.config
# low = 0 * 1000000000
# high = 90 * 1000000000


# low = 15 * 1000000000
# high = 75 * 1000000000

low = 10 * 1000000000
high = 50 * 1000000000


# low = 75 * 100000000
# high = 225 * 100000000

def get_rate(dir_name, f, key):
    rates = [l.split()[3] for l in open(os.path.join(dir_name, f), "r").readlines() if
             l.startswith(key) and len(l.split()) > 3]
    mb = []
    for r in rates:
        if len(r) < 3:
            continue
        m = float(r[:-2])
        if r.endswith("Mb"):
            mb.append(m)
        elif r.endswith("Kb"):
            mb.append(m / 1000)
    mb.sort(reverse=True)
    # print(mb)
    return mb


def analyse_bandwidth(dir_name):
    bandwidth_log = [f for f in os.listdir(dir_name) if f.startswith("bandwidth")]
    if len(bandwidth_log) == 0:
        return {}
    leader_ips = ["10.0.3.1",
                  "10.0.3.7",
                  "10.0.3.10",
                  "10.0.3.13",
                  "10.0.2.10"]
    leader_send_rate = []
    leader_receive_rate = []
    follower_send_rate = []
    follower_receive_rate = []
    client_send_rate = []
    client_receive_rate = []
    for f in bandwidth_log:
        send_rates = get_rate(dir_name, f, "Total send rate")[:15]
        receive_rates = get_rate(dir_name, f, "Total receive rate")[:15]
        ip = f.split("-")[2][:-4]
        if "server" in f:
            if ip in leader_ips:
                leader_send_rate.extend(send_rates)
                leader_receive_rate.extend(receive_rates)
            else:
                follower_send_rate.extend(send_rates)
                follower_receive_rate.extend(receive_rates)
        else:
            client_send_rate.extend(send_rates)
            client_receive_rate.extend(receive_rates)

    leader_send_avg = numpy.average(leader_send_rate)
    leader_receive_avg = numpy.average(leader_receive_rate)
    follower_receive_avg = numpy.average(follower_receive_rate)
    follower_send_avg = numpy.average(follower_send_rate)
    client_send_avg = numpy.average(client_send_rate)
    client_receive_avg = numpy.average(client_receive_rate)
    print("leader_receive_rate: " + str(leader_receive_avg))
    print("leader_send_rate: " + str(leader_send_avg))
    print("follower_receive_rate: " + str(follower_receive_avg))
    print("follower_send_rate: " + str(follower_send_avg))
    print("client_receive_rate: " + str(client_receive_avg))
    print("client_send_rate: " + str(client_send_avg))

    result = {
        "leader_receive_rate": leader_receive_avg,
        "leader_send_rate": leader_send_avg,
        "follower_receive_rate": follower_receive_avg,
        "follower_send_rate": follower_send_avg,
    }

    return result


def analyse_fastPath(dir_name):
    path = dir_name
    lists = os.listdir(path)
    total = 0
    fail = 0
    for f in lists:
        if f.endswith("coordinator.log"):
            lines = open(os.path.join(path, f), "r").readlines()
            for line in lines:
                items = line.split(",")
                if len(items) < 2:
                    continue
                if items[1] != "COMMIT":
                    continue
                fastResults = items[7][4:-1].split(" ")
                total += 1
                for p in fastResults:
                    if p.split(":")[1] == "false":
                        fail += 1
                        break
    rate = float(total - fail) / float(total)
    # print("fast path success rate: " + strrate)
    return rate


def analyse_optimization_count(dir_name, txn_map):
    lists = os.listdir(dir_name)
    total_txn = len(txn_map)
    result = {
        "reorder_success": 0,
        "reorder_fail": 0,
        "conditional_success": 0,
        "conditional_fail": 0,
        "forward_success": 0,
        "forward_fail": 0,
        "early_abort": 0,
    }
    for f in lists:
        if not f.endswith("coordinator.log"):
            continue
        lines = open(os.path.join(dir_name, f), "r").readlines()
        for line in lines[1:]:
            items = line.split(",")
            txnId = items[0][:-2]
            if txnId not in txn_map:
                continue
            if not txn_map[txnId]["priority"]:
                continue
            if items[2] == "true":
                if items[6] == "false":
                    result["reorder_success"] += 1
                else:
                    result["reorder_fail"] += 1
            if items[3] == "true":
                if items[8] == "true":
                    result["conditional_fail"] += 1
                else:
                    result["conditional_success"] += 1
            if items[7] == "true":
                if items[9] == "true":
                    result["forward_fail"] += 1
                else:
                    result["forward_success"] += 1
            if items[10] == "true":
                result["early_abort"] += 1

    for k in result:
        result[k] = (float(result[k]) / float(total_txn)) * 100
        # #print(k + ": " + str(result[k]))

    return result


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
                fast = False
                if len(items) == 8:
                    wait_num_t = int(items[2])
                    x = float(items[3]) / 1000000
                    y = float(items[4]) / 1000000
                    z = float(items[5]) / 1000000
                    t = int(items[6])
                    fast = items[7] == "true"
                if txn_id not in txn_map:
                    txn_map[txn_id] = [wait_num, wait_num_t, x, y, z, t, fast]
                txn_map[txn_id][0] = max(txn_map[txn_id][0], wait_num)
                txn_map[txn_id][1] = max(txn_map[txn_id][1], wait_num_t)
                txn_map[txn_id][2] = max(txn_map[txn_id][2], x)
                txn_map[txn_id][3] = max(txn_map[txn_id][3], y)
                txn_map[txn_id][4] = max(txn_map[txn_id][4], z)
                txn_map[txn_id][5] = txn_map[txn_id][5] + t
                txn_map[txn_id][6] = fast and txn_map[txn_id][6]

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
    num_txn = []
    client_num = 0
    client_starts = []
    for f in lists:
        if f.endswith(".statistic"):
            client_num += 1
            txn_count = 0
            first_start = sys.maxsize
            lines = open(os.path.join(path, f), "r").readlines()
            for line in lines:
                line = line.strip()
                if line.startswith("#"):
                    continue
                items = line.split(",")
                read_only = items[7] == "true"
                if read_only and args.noReadOnly:
                    continue
                txn_count += 1
                txn_id = items[0]
                commit = int(items[1]) == 1
                latency = float(items[2]) / 1000000  # ms
                start = float(items[3])
                end = float(items[4])

                exe_count = int(items[6])
                priority = False
                passTimestampTxn = 0
                passTimestampAbort = 0
                if len(items) > 8:
                    priority = items[8] == "true"
                if len(items) > 10:
                    passTimestampTxn = int(items[9])
                    passTimestampAbort = int(items[10])
                if start < min_start:
                    min_start = start
                if start < first_start:
                    first_start = start
                txn_map[txn_id] = {"commit": commit,
                                   "latency": latency,
                                   "start": start,
                                   "end": end,
                                   "priority": priority,
                                   "passTimestampTxn": passTimestampTxn,
                                   "passTimestampAbort": passTimestampAbort,
                                   "readOnly": read_only,
                                   "exeCount": exe_count}
            num_txn.append(txn_count)
            client_starts.append(first_start)
            if txn_count == 0:
                print(f + " dose not have txn")
    avg = numpy.average(num_txn)
    std = numpy.std(num_txn)
    client_starts.sort()
    diff = client_starts[-1] - client_starts[0]
    if diff / 1000000000.0 > 10:
        print(dir_name, "one client start 10s later than another client")
    # print(dir_name, num_txn, avg, std)
    print(dir_name)
    # print("avg txn num: " + str(avg) + " error: " + str(std))
    for txn_id, value in list(txn_map.items()):
        value["start"] = value["start"] - min_start
        if value["start"] < low or value["start"] > high:
            del txn_map[txn_id]
    # print("total valid txn", len(txn_map))
    return txn_map


def my_percentile(data, per):
    n = len(data)
    p = n * per / 100
    if p.is_integer():
        return sorted(data)[int(p)]
    else:
        return sorted(data)[int(math.ceil(p)) - 1]


def analyse_latency(txn_map):
    latency = []
    latency_high = []
    latency_low = []
    latency_retry = []
    latency_retry_high = []
    latency_retry_low = []
    for txn_id, value in txn_map.items():
        if value["commit"]:
            latency.append(value["latency"])
            latency_retry.append((value["latency"], value["exeCount"]))
            if value["priority"]:
                latency_high.append(value["latency"])
                latency_retry_high.append((value["latency"], value["exeCount"]))
            else:
                latency_low.append(value["latency"])
                latency_retry_low.append((value["latency"], value["exeCount"]))

    if len(latency) == 0:
        print("all transaction aborted")
        return {}

    median = numpy.percentile(latency, 50)
    p90 = numpy.percentile(latency, 90)
    p95 = numpy.percentile(latency, 95)
    p99 = numpy.percentile(latency, 99)
    p10 = numpy.percentile(latency, 10)
    avg = numpy.average(latency)

    median_retry = numpy.percentile(latency_retry, 50)
    p90_retry = my_percentile(latency_retry, 90)
    p95_retry = my_percentile(latency_retry, 95)
    p99_retry = my_percentile(latency_retry, 99)
    p10_retry = my_percentile(latency_retry, 10)

    if args.resultDir is not None:
        # print("10 per (ms) high: %s" + str(p10))
        # print("median (ms) high: " + str(median))
        # print("90 per (ms) high: " + str(p90))
        # print("95 per (ms) high: " + str(p95))
        # print("99 per (ms) high: " + str(p99))
        # print("avg (ms) high: " + str(avg))

        print("10 per (ms): %s, %s" % (str(p10), (p10_retry,)))
        print("median (ms): %s, %s" % (str(median), (median_retry,)))
        print("90 per (ms): %s, %s" % (str(p90), (p90_retry,)))
        print("95 per (ms): %s, %s" % (str(p95), (p95_retry,)))
        print("99 per (ms): %s, %s" % (str(p99), (p99_retry,)))
        print("avg (ms): " + str(avg))

    # print("10 per (ms): " + str(p10))
    # print("median (ms): " + str(median))
    # print("90 per (ms): " + str(p90))
    # print("95 per (ms): " + str(p95))
    # print("99 per (ms): " + str(p99))
    # print("avg (ms): " + str(avg))

    latency.sort()

    result = {"median": median,
              "p90": p90,
              "p95": p95,
              "p10": p10,
              "p99": p99,
              "avg": avg,
              # "latency": latency
              }

    if len(latency_high) > 0:
        median = numpy.percentile(latency_high, 50)
        p90 = numpy.percentile(latency_high, 90)
        p95 = numpy.percentile(latency_high, 95)
        p99 = numpy.percentile(latency_high, 99)
        p10 = numpy.percentile(latency_high, 10)
        avg = numpy.average(latency_high)

        median_retry_high = numpy.percentile(latency_retry_high, 50)
        p90_retry_high = my_percentile(latency_retry_high, 90)
        p95_retry_high = my_percentile(latency_retry_high, 95)
        p99_retry_high = my_percentile(latency_retry_high, 99)
        p10_retry_high = my_percentile(latency_retry_high, 10)

        latency_high.sort()
        result["median_high"] = median
        result["p90_high"] = p90
        result["p95_high"] = p95
        result["p99_high"] = p99
        result["p10_high"] = p10
        result["avg_high"] = avg
        # result["latency_high"] = latency_high

        if args.resultDir is not None:
            # print("10 per (ms) high: " + str(p10))
            # print("median (ms) high: " + str(median))
            # print("90 per (ms) high: " + str(p90))
            # print("95 per (ms) high: " + str(p95))
            # print("99 per (ms) high: " + str(p99))
            # print("avg (ms) high: " + str(avg))
            print("10 per (ms) high: %s, %s" % (str(p10), (p10_retry_high,)))
            print("median (ms) high: %s, %s" % (str(median), (median_retry_high,)))
            print("90 per (ms) high: %s, %s" % (str(p90), (p90_retry_high,)))
            print("95 per (ms) high: %s, %s" % (str(p95), (p95_retry_high,)))
            print("99 per (ms) high: %s, %s" % (str(p99), (p99_retry_high,)))
            print("avg (ms) high: " + str(avg))
    if len(latency_low) > 0:
        median = numpy.percentile(latency_low, 50)
        p90 = numpy.percentile(latency_low, 90)
        p95 = numpy.percentile(latency_low, 95)
        p99 = numpy.percentile(latency_low, 99)
        p10 = numpy.percentile(latency_low, 10)
        avg = numpy.average(latency_low)

        median_retry_low = numpy.percentile(latency_retry_low, 50)
        p90_retry_low = my_percentile(latency_retry_low, 90)
        p95_retry_low = my_percentile(latency_retry_low, 95)
        p99_retry_low = my_percentile(latency_retry_low, 99)
        p10_retry_low = my_percentile(latency_retry_low, 10)

        latency_low.sort()
        result["median_low"] = median
        result["p90_low"] = p90
        result["p95_low"] = p95
        result["p99_low"] = p99
        result["p10_low"] = p10
        result["avg_low"] = avg
        # result["latency_low"] = latency_low
        if args.resultDir is not None:
            # print("10 per (ms) low: " + str(p10))
            # print("median (ms) low: " + str(median))
            # print("90 per (ms) low: " + str(p90))
            # print("95 per (ms) low: " + str(p95))
            # print("99 per (ms) low: " + str(p99))
            # print("avg (ms) low: " + str(avg))
            print("10 per (ms) low: %s, %s" % (str(p10), (p10_retry_low,)))
            print("median (ms) low: %s, %s" % (str(median), (median_retry_low,)))
            print("90 per (ms) low: %s, %s" % (str(p90), (p90_retry_low,)))
            print("95 per (ms) low: %s, %s" % (str(p95), (p95_retry_low,)))
            print("99 per (ms) low: %s, %s" % (str(p99), (p99_retry_low,)))
            print("avg (ms) low: " + str(avg))

    return result


def analyse_retry(txn_map):
    low_retry = []
    high_retry = []
    result = {}
    for txn_id, value in txn_map.items():
        if not value["commit"]:
            continue
        if value["priority"]:
            high_retry.append(value["exeCount"])
        else:
            low_retry.append(value["exeCount"])
    if len(low_retry) > 0:
        median = numpy.percentile(low_retry, 50)
        p90 = numpy.percentile(low_retry, 90)
        p95 = numpy.percentile(low_retry, 95)
        p99 = numpy.percentile(low_retry, 99)
        p10 = numpy.percentile(low_retry, 10)
        avg = numpy.average(low_retry)

        if args.resultDir is not None:
            print("retry 10 per (ms) low: " + str(p10))
            print("retry median (ms) low: " + str(median))
            print("retry 90 per (ms) low: " + str(p90))
            print("retry 95 per (ms) low: " + str(p95))
            print("retry 99 per (ms) low: " + str(p99))
            print("retry avg (ms) low: " + str(avg))

        result = {
            "retry_median_low": median,
            "retry_p90_low": p90,
            "retry_p95_low": p95,
            "retry_p99_low": p99,
            "retry_p10_low": p10,
            "retry_avg_low": avg,
        }
    if len(high_retry) > 0:
        median = numpy.percentile(high_retry, 50)
        p90 = numpy.percentile(high_retry, 90)
        p95 = numpy.percentile(high_retry, 95)
        p99 = numpy.percentile(high_retry, 99)
        p10 = numpy.percentile(high_retry, 10)
        avg = numpy.average(high_retry)

        if args.resultDir is not None:
            print("retry 10 per (ms) high: " + str(p10))
            print("retry median (ms) high: " + str(median))
            print("retry 90 per (ms) high: " + str(p90))
            print("retry 95 per (ms) high: " + str(p95))
            print("retry 99 per (ms) high: " + str(p99))
            print("retry avg (ms) high: " + str(avg))

        result["retry_median_high"] = median
        result["retry_p90_high"] = p90
        result["retry_p95_high"] = p95
        result["retry_p99_high"] = p99
        result["retry_p10_high"] = p10
        result["retry_avg_high"] = avg

    return result


def analyse_throughput(txn_map):
    min_time = sys.maxsize
    max_time = -sys.maxsize - 1
    count = 0
    count_high = 0
    count_low = 0
    total_count = 0
    total_executed_txn = 0
    total_executed_txn_high = 0
    total_executed_txn_low = 0
    pass_timestamp_abort = 0
    pass_timestamp_abort_high = 0
    pass_timestamp_abort_low = 0
    pass_timestamp_txn = 0
    pass_timestamp_txn_high = 0
    pass_timestamp_txn_low = 0
    for txn_id, value in txn_map.items():
        if value["start"] < min_time:
            min_time = value["start"]

        if value["start"] > max_time:
            max_time = value["start"]

        total_count += 1
        # total_count += value["exeCount"]
        total_executed_txn += value["exeCount"] + 1
        if "passTimestampAbort" in value:
            pass_timestamp_abort += value["passTimestampAbort"]
            pass_timestamp_txn += value["passTimestampTxn"]
        if value["priority"]:
            total_executed_txn_high += value["exeCount"] + 1
            if "passTimestampAbort" in value:
                pass_timestamp_txn_high += value["passTimestampTxn"]
                pass_timestamp_abort_high += value["passTimestampAbort"]
        else:
            total_executed_txn_low += value["exeCount"] + 1
            if "passTimestampAbort" in value:
                pass_timestamp_txn_low += value["passTimestampTxn"]
                pass_timestamp_abort_low += value["passTimestampAbort"]

        if value["commit"]:
            count += 1
            # total_count += 1
            if value["priority"]:
                count_high += 1
            else:
                count_low += 1

    throughput = float(count * 1000000000) / (max_time - min_time)
    all_thro = float(total_executed_txn * 1000000000) / (max_time - min_time)

    pass_time_abort_percentage = float(pass_timestamp_abort) / float(total_executed_txn)
    pass_time_txn_percentage = float(pass_timestamp_txn) / float(total_executed_txn)

    pass_time_abort_percentage_high = 0
    pass_time_txn_percentage_high = 0
    if total_executed_txn_high != 0:
        pass_time_abort_percentage_high = float(pass_timestamp_abort_high) / float(total_executed_txn_high)
        pass_time_txn_percentage_high = float(pass_timestamp_txn_high) / float(total_executed_txn_high)

    pass_time_abort_percentage_low = 0
    pass_time_txn_percentage_low = 0
    if total_executed_txn_low != 0:
        pass_time_abort_percentage_low = float(pass_timestamp_abort_low) / float(total_executed_txn_low)
        pass_time_txn_percentage_low = float(pass_timestamp_txn_low) / float(total_executed_txn_low)

    throughput_high = float(count_high * 1000000000) / (max_time - min_time)
    throughput_low = float(count_low * 1000000000) / (max_time - min_time)
    if args.resultDir is not None:
        print("commit throughput high (txn/s): " + str(throughput_high))
        print("commit throughput low (txn/s): " + str(throughput_low))
        print("throughput(txn/s) (no retry included): " + str(throughput))
        print("throughput(txn/s) (retry included): " + str(all_thro))
        print("total executed txn: " + str(total_executed_txn))
        print("pass timestamp abort: " + str(pass_timestamp_abort))
        print("pass timestamp abort (%): " + str(pass_time_abort_percentage))
        print("pass timestamp txn: " + str(pass_timestamp_txn))
        print("pass timestamp txn (%): " + str(pass_time_txn_percentage))

        print("total executed txn high: " + str(total_executed_txn_high))
        print("pass timestamp abort high: " + str(pass_timestamp_abort_high))
        print("pass timestamp abort high (%): " + str(pass_time_abort_percentage_high))
        print("pass timestamp txn high: " + str(pass_timestamp_txn_high))
        print("pass timestamp txn high (%): " + str(pass_time_txn_percentage_high))

        print("total executed txn low: " + str(total_executed_txn_low))
        print("pass timestamp abort low: " + str(pass_timestamp_abort_low))
        print("pass timestamp abort low(%): " + str(pass_time_abort_percentage_low))
        print("pass timestamp txn low: " + str(pass_timestamp_txn_low))
        print("pass timestamp txn low(%): " + str(pass_time_txn_percentage_low))

    result = {"throughput_low": throughput_low,
              "throughput_high": throughput_high,
              "throughput_retry": all_thro,
              "throughput": throughput,
              "pass_time_txn_percentage": pass_time_txn_percentage,
              "pass_time_abort_percentage": pass_time_abort_percentage,
              "pass_time_txn_percentage_high": pass_time_txn_percentage_high,
              "pass_time_abort_percentage_high": pass_time_abort_percentage_high,
              "pass_time_txn_percentage_low": pass_time_txn_percentage_low,
              "pass_time_abort_percentage_low": pass_time_abort_percentage_low}

    return result


def analyse_abort_rate(txn_map):
    commit = 0
    commit_high = 0
    commit_low = 0
    count = 0
    count_high = 0
    count_low = 0
    for txn_id, value in txn_map.items():
        count += 1
        # count += value["exeCount"]
        if value["priority"]:
            count_high += 1
            # count_high += value["exeCount"]
        else:
            count_low += 1
            # count_low += value["exeCount"]
        if value["commit"]:
            commit += 1
            if value["priority"]:
                commit_high += 1
            else:
                commit_low += 1

    abort_rate = 1 - float(commit) / count

    # print("Abort rate: " + str(abort_rate))

    if commit_high == 0 or commit_low == 0:
        return {"abort_rate": abort_rate, "abort_rate_high": 0, "abort_rate_low": 0}

    abort_high_rate = 1 - float(commit_high) / count_high
    abort_low_rate = 1 - float(commit_low) / count_low
    if args.resultDir is not None:
        print("Abort rate high: " + str(abort_high_rate))
        print("Abort rate low: " + str(abort_low_rate))
    result = {"abort_rate": abort_rate, "abort_rate_low": abort_low_rate, "abort_rate_high": abort_high_rate}
    return result


# def analyse_fast_prepare_rate(txn_map):
#     count_high = 0
#     count_low = 0
#     fast_prepare_high = 0
#     fast_prepare_low = 0
#     for txn_id, value in txn_map.items():
#         if value["readOnly"]:
#             continue
#         if value["priority"]:
#             count_high += 1
#             if value["fastPrepare"]:
#                 fast_prepare_high += 1
#         else:
#             count_low += 1
#             if value["fastPrepare"]:
#                 fast_prepare_low += 1
#
#     fast_prepare_rate = float(fast_prepare_high + fast_prepare_low) / (count_low + count_high)
#     print("fast path success rate: " + str(fast_prepare_rate))
#
#     if count_high == 0 or count_low == 0:
#         return fast_prepare_rate, 0, 0
#
#     fast_prepare_rate_high = float(fast_prepare_high) / count_high
#     fast_prepare_rate_low = float(fast_prepare_low) / count_low
#     print("fast path success rate high: " + str(fast_prepare_rate_high))
#     print("fast path success rate low: " + str(fast_prepare_rate_low))
#     return fast_prepare_rate, fast_prepare_rate_low, fast_prepare_rate_high


def analyse(dir_name):
    l = dir_name.split(".")[0].split("-")[:-1]
    sn = '-'.join(l) + ".json"
    sf = open(sn)
    setting = json.load(sf)
    clientN = setting["clients"]["nums"]
    n = len([f for f in os.listdir(dir_name)
             if f.endswith('.statistic') and os.path.isfile(os.path.join(dir_name, f))])
    if n != clientN:
        for f in os.listdir(dir_name):
            os.remove(os.path.join(dir_name, f))
        os.rmdir(dir_name)
        print(dir_name + " does not contain *.statistic file, requires " + str(clientN) + " has " + str(n))
        # print(dir_name)
        return
    if n == 0:
        # shutil.rmtree(dir_name)
        return
    # print(clientN, dir_name, setting["experiment"]["varExp"])
    path = dir_name
    # fastPathSuccessRate = analyse_fast_prepare_rate(path)
    # analyse_waiting(path)
    txn_map = load_statistic(path)
    result = analyse_latency(txn_map)

    abort_rate_result = analyse_abort_rate(txn_map)
    for k in abort_rate_result:
        result[k] = abort_rate_result[k]

    throughput_result = analyse_throughput(txn_map)
    for k in throughput_result:
        result[k] = throughput_result[k]

    optimization_count = analyse_optimization_count(dir_name, txn_map)
    for k in optimization_count:
        result[k] = optimization_count[k]

    retry = analyse_retry(txn_map)
    for k in retry:
        result[k] = retry[k]

    bandwidth_result = analyse_bandwidth(dir_name)
    for k in bandwidth_result:
        result[k] = bandwidth_result[k]

    file_name = os.path.basename(path)
    if args.noReadOnly:
        with open(file_name + ".resultNR", "w") as f:
            json.dump(result, f, indent=4)
    else:
        with open(file_name + ".result", "w") as f:
            json.dump(result, f, indent=4)


def print_result(result, prefix):
    print("result for " + prefix[:-1])
    for key in result:
        if key.endswith("high"):
            print(key + ": " + str(result[key]['mean']) + " error: " + str(result[key]["error"]))
    for key in result:
        if key.endswith("low"):
            print(key + ": " + str(result[key]['mean']) + " error: " + str(result[key]["error"]))
    print("--------------------------------------------")


def incRate(base, path, prefix):
    lists = os.listdir(path)
    result = {"p95_high": [], "p95_low": []}
    suffix = ".result"
    if args.noReadOnly:
        suffix += "NR"
    result_files = [f for f in lists if f.startswith(prefix) and f.endswith(suffix)]
    if len(result_files) == 0:
        return

    base_high = base["p95_high"]["mean"]
    base_low = base["p95_low"]["mean"]
    for f in result_files:
        print(f)
        fp = open(os.path.join(path, f), "r")
        data = json.load(fp)
        fp.close()
        cur_high = data["p95_high"]
        cur_low = data["p95_low"]
        print(cur_high, cur_low)
        result["p95_high"].append((cur_high - base_high) / base_high)
        result["p95_low"].append((cur_low - base_low) / base_low)

    for key in result:
        value = result[key]
        mean = numpy.average(value)
        error = 1.96 * (numpy.std(value) / math.sqrt(len(value)))
        result[key] = {"mean": mean, "error": error}

    cf = open(os.path.join(path, prefix[:-1] + ".json"), "r")
    config = json.load(cf)
    result["config"] = config
    file_name = prefix[:-1] + ".finalInc"
    if args.noReadOnly:
        file_name = prefix[:-1] + ".finalNRInc"
    with open(file_name, "w") as f:
        json.dump(result, f, indent=4)


def error_bar(path, prefix):
    lists = os.listdir(path)
    result = {}
    suffix = ".result"
    if args.noReadOnly:
        suffix += "NR"
    result_files = [f for f in lists if f.startswith(prefix) and f.endswith(suffix)]
    if len(result_files) == 0:
        return
    for f in result_files:
        fp = open(os.path.join(path, f), "r")
        data = json.load(fp)
        fp.close()
        for key in data:
            value = data[key]
            if key not in result:
                result[key] = []
            result[key].append(value)

    for key in result:
        value = result[key]
        if isinstance(value[0], list):
            result[key] = list(itertools.chain(*result[key]))
            continue
        mean = numpy.average(value)
        error = 1.96 * (numpy.std(value) / math.sqrt(len(value)))

        result[key] = {"mean": mean, "error": error}
    # print_result(result, prefix)
    cf = open(os.path.join(path, prefix[:-1] + ".json"), "r")
    config = json.load(cf)
    result["config"] = config
    file_name = prefix[:-1] + ".final"
    if args.noReadOnly:
        file_name = prefix[:-1] + ".finalNR"
    with open(file_name, "w") as f:
        json.dump(result, f, indent=4)


def result_file_exist(dr):
    result_file = dr + ".result"
    if args.noReadOnly:
        result_file = dr + ".resultNR"
    if os.path.exists(result_file):
        return True
    return False


def analyse_file():
    path = os.getcwd()
    lists = os.listdir(path)
    for f in args.configFile:
        print(f)
        prefix = f.split(".")[0] + "-"
        dLists = [d for d in lists if d.startswith(prefix) and os.path.isdir(os.path.join(path, d))]
        for d in dLists:
            if not args.force and result_file_exist(d):
                continue
            analyse(d)
        error_bar(path, prefix)


def main():
    if args.resultDir is not None:
        for dr in args.resultDir:
            if not args.force and result_file_exist(dr):
                continue
            analyse(dr)
        return
    if args.configFile is not None:
        analyse_file()
        return

    path = os.getcwd()
    lists = os.listdir(path)
    for f in lists:
        if os.path.isdir(os.path.join(path, f)):
            if not args.force and result_file_exist(f):
                continue
            analyse(f)

    for f in lists:
        if f.endswith(".json"):
            prefix = f.split(".")[0] + "-"
            error_bar(path, prefix)

    if args.increase:
        for f in lists:
            if f.endswith(".json"):
                prefix = f.split(".")[0]
                if prefix.endswith("txnRate-10"):
                    continue
                idx = prefix.rindex('-')
                baseF = prefix[0:idx] + "-" + "10.final"
                if args.noReadOnly:
                    baseF = prefix[0:idx] + "-" + "10.finalNR"
                fp = open(os.path.join(path, baseF), "r")
                base = json.load(fp)
                prefix += '-'
                incRate(base, path, prefix)


if __name__ == "__main__":
    main()
