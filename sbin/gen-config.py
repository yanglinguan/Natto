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

arg_parser.add_argument('-a', '--azureIps', dest='azureIps', nargs='?',
                        help='azure servers public and private ips file', required=False)

args = arg_parser.parse_args()

if not os.path.exists(args.directory):
    os.makedirs(args.directory)

# Reads machine configurations
config_file = open(args.config, "r")
config_option = json.load(config_file)
config_file.close()
config_name = ["servers", "clients", "experiment"]

azure_machine_name_ips = {}


def parse_azure_ips():
    ip_file = open(args.azureIps, 'r')
    all_lines = ip_file.readlines()
    ip_file.close()

    for line in all_lines:
        if line.startswith("#"):
            continue
        if line.startswith("--------"):
            continue

        ip_list = line.rstrip('\n').split()
        vm_name, ip_pub, ip_pri = ip_list[0], ip_list[1], ip_list[2]
        if vm_name not in azure_machine_name_ips:
            azure_machine_name_ips[vm_name] = {"public": "", "private": ""}
        azure_machine_name_ips[vm_name]["public"] = ip_pub
        azure_machine_name_ips[vm_name]["private"] = ip_pri


def config_server():
    server = config_option["server"]
    server["coordMachines"] = []
    server["coordMachines_pub"] = []
    if args.azureIps is not None:
        server["machines"] = []
        server["machines_pub"] = []
        for m_name in config_option["server_machines"]:
            server["machines"].append(azure_machine_name_ips[m_name]["private"])
            server["machines_pub"].append(azure_machine_name_ips[m_name]["public"])
        if "coord_machines" in config_option:
            for m_name in config_option["coord_machines"]:
                server["coordMachines"].append(azure_machine_name_ips[m_name]["private"])
                server["coordMachines_pub"].append(azure_machine_name_ips[m_name]["public"])
    else:
        server["machines"] = config_option["server_machines"]
        server["machines_pub"] = config_option["server_machines"]
        if "coord_machines" in config_option:
            server["coordMachines"] = config_option["coord_machines"]
            server["coordMachines_pub"] = config_option["coord_machines"]
    return server


def config_client(nums):
    client_list = []
    azure_client_machines_pri = []
    azure_client_machines_pub = []
    azure_networkMeasure_machines_pri = []
    azure_networkMeasure_machines_pub = []
    if args.azureIps is not None:
        for m_name in config_option["client_machines"]:
            azure_client_machines_pri.append(azure_machine_name_ips[m_name]["private"])
            azure_client_machines_pub.append(azure_machine_name_ips[m_name]["public"])
        for m_name in config_option["networkMeasureMachines"]:
            azure_networkMeasure_machines_pri.append(azure_machine_name_ips[m_name]["private"])
            azure_networkMeasure_machines_pub.append(azure_machine_name_ips[m_name]["public"])
    for n in nums:
        client_config = {
            "nums": n,
            "machines": config_option["client_machines"] if args.azureIps is None else azure_client_machines_pri,
            "machines_pub": config_option["client_machines"] if args.azureIps is None else azure_client_machines_pub,
            "networkMeasurePortBase": config_option["networkMeasurePortBase"],
            "networkMeasureMachines": config_option["networkMeasureMachines"] if args.azureIps is None else azure_networkMeasure_machines_pri,
            "networkMeasureMachines_pub": config_option["networkMeasureMachines"] if args.azureIps is None else azure_networkMeasure_machines_pub,
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

        #f = config["experiment"]["workload"]["type"]+ "_" + str(config["experiment"]["retry"]["interval"]) + "_" + str(config["experiment"]["retry"]["maxSlot"]) + "_" + config["experiment"]["varExp"] + ".json"
        f = config["experiment"]["workload"]["type"]+ "_" + config["experiment"]["varExp"] + ".json"
        #f = config["experiment"]["varExp"] + ".json"
        with open(os.path.join(args.directory, f), "w") as fp:
            json.dump(config, fp, indent=4, sort_keys=True)


def main():
    if args.azureIps:
        parse_azure_ips()
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
                    if x_name == "latency_variance" and val == "off":
                        val = 0
                    ev["varExp"] += str(val)
                    final_exp_list.append(ev)
            config_list.append(final_exp_list)
        output(config_list, var_client)


if __name__ == "__main__":
    main()
