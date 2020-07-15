import argparse
import json
import itertools
import os

arg_parser = argparse.ArgumentParser(description="generate config file.")

# Cluster configuration file
arg_parser.add_argument('-m', '--machines', dest='machines', nargs='?',
                        help='machine configuration file', required=True)

arg_parser.add_argument('-d', '--directory', dest='directory', nargs='?',
                        help='directory for storing config file', required=True)

args = arg_parser.parse_args()

if not os.path.exists(args.directory):
    os.makedirs(args.directory)

# Reads machine configurations
machine_config_file = open(args.machines, "r")
machine_config = json.load(machine_config_file)
machine_config_file.close()

# server config
server_nums = 15
server_partitions = 3
server_replicationFactor = 3
server_failure = 1
server_raftPortBase = 20000
server_rpcPortBase = 10000
server = {
    "nums": server_nums,
    "partitions": server_partitions,
    "replicationFactor": server_replicationFactor,
    "raftPortBase": server_raftPortBase,
    "rpcPortBase": server_rpcPortBase,
    "machines": machine_config["server_machines"]
}

config_name = ["servers", "clients", "experiment"]
config_list = [[server]]

# client config
client_nums = [5]
clients = []
for c_num in client_nums:
    client = {
        "nums": c_num,
        "machines": machine_config["client_machines"]
    }
    clients.append(client)

config_list.append(clients)

exp = {
    "readOnly": True,
    "checkWaiting": False,
    "replication": True,
    "totalKey": 10000000,
    "RPCPoolSize": 0,
    "keySize": 64,
    "duration": "90s",
    "totalTxn": 1000,
    "targetRate": 100,
    "dynamicLatency": {
        "mode": False,
        "probeWindowLen": "1s",
        "probeWindowMinSize": 10,
        "probeInterval": "10ms",
        "blocking": False,
        "probeTime": False
    },
    "workload": {
        "type": "ycsbt",
        "retwis": {
            "addUserRatio": 5,
            "followUnfollowRatio": 15,
            "postTweetRatio": 30,
            "loadTimelineRatio": 50
        },
        "highPriority": 100
    },
    "seed": 0,
    "queueLen": 10240,
    "retry": {
        "mode": "exp",
        "interval": "10ms",
        "maxRetry": 20,
        "maxSlot": 32
    },
    "latency": {
        "variance": machine_config["variance"],
        "distribution": machine_config["distribution"],
        "oneWayDelay": machine_config["oneWayDelay"]
    },
    "ssh": {
        "username": "l69yang",
        "identity": ""
    },
    "runDir": "/ssd1/carousel-gts/"
}

# exp config
# variable
var_names = [
    "mode",
    "fastPath",
    "delay",
    "txnSize",
    "openLoop",
    "txnRate",
    "zipfAlpha",
    "timeWindow",
    "conditionalPrepare",
    "optimisticReorder",
]
var_value = [
    ["gts", "occ"],  # "mode"
    [False],  # fastPath
    ["10ms"],  # delay
    [2],  # txnSize
    [True],  # openloop
    [100, 200, 400, 600, 800],  # txnRate
    [0.5, 0.75, 0.85, 0.95],  # zipf
    ["10ms"],  # windowSize
    [False],  # conditionalPrepare
    [False],  # optimisticReorder
]

combo = list(itertools.product(*var_value))

print(combo)

eList = []

for value in combo:
    i = 0
    e = exp.copy()
    fileName = ""
    for v in value:
        name = var_names[i]
        fileName += name
        fileName += "(" + str(v) + ")-"
        i += 1
        e[name] = v
    fileName = fileName[:-1]
    fileName += ".json"
    fileName = os.path.join(args.directory, fileName)
    e["fileName"] = fileName
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
    f = config["experiment"]["fileName"]
    with open(f, "w") as fp:
        json.dump(config, fp, indent=4, sort_keys=True)
