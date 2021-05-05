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
config_name = ["servers", "clients", "experiment"]


def config_server():
    server = config_option["server"]
    server["machines"] = config_option["server_machines"]
    return server


def config_client(nums):
    client_list = []
    for n in nums:
        client_config = {
            "nums": n,
            "machines": config_option["client_machines"],
            "networkMeasurePortBase": config_option["networkMeasurePortBase"],
            "networkMeasureMachines": config_option["networkMeasureMachines"]
        }
        client_list.append(client_config)
    return client_list


def config_exp():
    default_exp = config_option["default_exp"]
    default_exp["latency"] = config_option["latency"]

    var = config_option["var_exp"]
    eList = []
    for n in var:
        var_value = var[n]
        ev = copy.deepcopy(default_exp)
        for k in var_value:
            val = var_value[k]
            assign_value(ev, k, val)
        ev['varExp'] = n
        eList.append(ev)
    if len(eList) == 0:
        eList.append(default_exp)
    return eList


def assign_value(exp_config, key, val):
    name_list = key.split("_")
    ev = exp_config
    for ke in name_list[:-1]:
        ev = ev[ke]
    ev[name_list[-1]] = val


def output(config_list, var_client):
    config_combo = list(itertools.product(*config_list))
    for combo in config_combo:
        i = 0
        config = {}
        for c in combo:
            name = config_name[i]
            config[name] = c
            i += 1
        if var_client:
            exp = copy.deepcopy(config["experiment"])
            exp["varExp"] += "-client_nums-" + str(config["clients"]["nums"])
            config["experiment"] = exp

        f = config["experiment"]["varExp"] + ".json"
        with open(os.path.join(args.directory, f), "w") as fp:
            json.dump(config, fp, indent=4, sort_keys=True)


def main():
    x_axis = config_option["x_axis"]
    server = config_server()
    for x_name in x_axis:
        config_list = [[server]]
        client_config = config_client([config_option["client_nums"]])
        exp_list = config_exp()

        x_values = x_axis[x_name]
        var_client = x_name == "client_nums"
        if var_client:
            client_config = config_client(x_values)
            config_list.append(client_config)
            config_list.append(exp_list)
        else:
            config_list.append(client_config)
            final_exp_list = []
            for exp in exp_list:
                for val in x_values:
                    ev = copy.deepcopy(exp)
                    if not var_client:
                        assign_value(ev, x_name, val)
                    ev["varExp"] += "-" + x_name + "-"
                    if x_name == "zipfAlpha":
                        val *= 100
                        val = int(val)
                    ev["varExp"] += str(val)
                    final_exp_list.append(ev)
            config_list.append(final_exp_list)
        output(config_list, var_client)


if __name__ == "__main__":
    main()
