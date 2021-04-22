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
eList = []


def assign_value(exp_config, key, val):
    name_list = key.split("_")
    ev = exp_config
    for ke in name_list[:-1]:
        ev = ev[ke]
    ev[name_list[-1]] = val


for name in var:
    var_value = var[name]
    e = copy.deepcopy(exp)
    for k in var_value:
        v = var_value[k]
        assign_value(e, k, v)
    e['varExp'] = name
    eList.append(e)

x_names = []
x_values = []
x_axis = config_option["x_axis"]
for name in x_axis:
    x_names.append(name)
    x_values.append(x_axis[name])

combo = list(itertools.product(*x_values))

final_exp = []
for ex in eList:
    for value in combo:
        i = 0
        e = copy.deepcopy(ex)
        for v in value:
            name = x_names[i]
            i += 1
            assign_value(e, name, v)
            e["varExp"] += "-" + name + "-"
            e["varExp"] += str(v)
        final_exp.append(e)

config_list.append(final_exp)

config_combo = list(itertools.product(*config_list))

for combo in config_combo:
    i = 0
    config = {}
    for c in combo:
        name = config_name[i]
        config[name] = c
        i += 1

    f = config["experiment"]["varExp"] + ".json"

    with open(os.path.join(args.directory, f), "w") as fp:
        json.dump(config, fp, indent=4, sort_keys=True)
