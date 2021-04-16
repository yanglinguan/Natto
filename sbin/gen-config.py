#!/usr/bin/python
import argparse
import json
import itertools
import os
import copy

arg_parser = argparse.ArgumentParser(description="generate config file.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=True)

arg_parser.add_argument('-d', '--directory', dest='directory', nargs='?',
                        help='directory for storing config file', required=True)

args = arg_parser.parse_args()

fcounter = 0
if not os.path.exists(args.directory):
    os.makedirs(args.directory)

for f in os.listdir(args.directory):
    if f.startswith(args.directory) and f.endswith(".json"):
        n = int((f.split(".")[0]).split('-')[-1])
        if n > fcounter:
            fcounter = n

# Reads machine configurations
config_file = open(args.config, "r")
config_option = json.load(config_file)
config_file.close()

server = config_option["server"]
server["machines"] = config_option["server_machines"]

config_name = ["servers", "clients", "experiment"]
config_list = [[server]]

client_nums = config_option["client_nums"]
clients = []
for c_num in client_nums:
    client = {
        "nums": c_num,
        "machines": config_option["client_machines"]
    }
    clients.append(client)

config_list.append(clients)

exp = config_option["fix_exp"]
exp["latency"] = config_option["latency"]

var = config_option["var_exp"]
var_names = []
var_value = []
for name in var:
    var_names.append(name)
    var_value.append(var[name])

combo = list(itertools.product(*var_value))

shortName = {
    "optimisticReorder": "oR",
    "conditionalPrepare": "cP",
    "workloadhighPriority": "hP",
    "retrymaxRetry": "maxRetry",
    "zipfAlpha": "zipf",
    "workloadtype": "workload",
    "fastPath": "fP",
    "openLoop": "oL",
    "timeWindow": "tw",
    "readBeforeCommitReplicate": "rbcr",
    "forwardReadToCoord": "frtc",
    "popular": "pop",
}

eList = []

for value in combo:
    i = 0
    e = copy.deepcopy(exp)
    # fileName = ""
    for v in value:
        name = var_names[i]
        i += 1
        items = name.split("_")
        n = "".join(items)
        if len(items) == 2:
            e[items[0]][items[1]] = v
        else:
            e[name] = v
        if n in shortName:
            n = shortName[n]
        # fileName += n
        x = v
        if name == "zipfAlpha":
            x = int(v*100)
        # fileName += "_" + str(x) + "-"
    # e["fileName"] = fileName
    eList.append(e)

config_list.append(eList)

config_combo = list(itertools.product(*config_list))

for combo in config_combo:
    i = 0
    config = {}
    for c in combo:
        name = config_name[i]
        config[name] = c
        i += 1

    f = args.directory+ "-" + str(fcounter) + ".json"
    fcounter += 1
    config["experiment"]["fileName"] = f

    with open(os.path.join(args.directory, f), "w") as fp:
        json.dump(config, fp, indent=4, sort_keys=True)
