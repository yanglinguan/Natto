#!/usr/bin/python
import argparse
import json

arg_parser = argparse.ArgumentParser(description="generate config file.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=True)

args = arg_parser.parse_args()

short_name = {
    "zipf": "experiment_zipfAlpha",
    "tw": "experiment_timeWindow",
    "txnSize": "experiment_txnSize",
    "mode": "experiment_mode",
    "high priority": "experiment_workload_highPriority",
    "conditional prepare": "experiment_conditionalPrepare",
    "lor": "experiment_readBeforeCommitReplicate",
    "ror": "experiment_forwardReadToCoord",
    "txnRate": "experiment_txnRate",
    "client num": "clients_nums",
}


def getValue(setting, key):
    if key in short_name:
        key = short_name[key]
    keyList = key.split("_")
    val = setting
    for k in keyList:
        val = val[k]
    return val


config_file = open(args.config, "r")
config_option = json.load(config_file)
config_file.close()

for name in short_name:
    print(name + ": " + str(getValue(config_option, short_name[name])))
