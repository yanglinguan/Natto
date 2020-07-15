#!/usr/bin/python
import argparse
import json
import itertools
import os

arg_parser = argparse.ArgumentParser(description="generate config file.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=True)

arg_parser.add_argument('-d', '--directory', dest='directory', nargs='?',
                        help='directory for storing config file', required=True)

args = arg_parser.parse_args()

if not os.path.exists(args.directory):
    os.makedirs(args.directory)

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

# print(combo)

eList = []

for value in combo:
    i = 0
    e = exp.copy()
    fileName = ""
    for v in value:
        name = var_names[i]
        fileName += name
        fileName += "_" + str(v) + "-"
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
