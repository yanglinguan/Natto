#!/usr/bin/python
import json
import argparse
import os

default_user = os.environ['USER']

arg_parser = argparse.ArgumentParser(description="Set delays among dataCenters.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='cluster configuration file', required=True)
arg_parser.add_argument('-u', '--user', dest='user', nargs='?',
                        help='username', default=default_user)
arg_parser.add_argument('-b', '--bandwidth', dest='bandwidth', nargs='?',
                        help='bandwidth', default='1000Mbps')
arg_parser.add_argument('-d', '--dev', dest='dev', nargs='?',
                        help='network interface device name', default='eno1')
arg_parser.add_argument('-p', '--parallel', action='store_true')

args = arg_parser.parse_args()

user = args.user
bandwidth = args.bandwidth
dev = args.dev
suffix = ''
if args.parallel:
    suffix = " &"

# Reads configurations
config_file = open(args.config, "r")
config = json.load(config_file)
config_file.close()

dc_ip_map = {}
machines = config["servers"]["machines"]
replicationFactor = config["servers"]["replicationFactor"]
serverNum = config["servers"]["nums"]
dcNum = config["servers"]["dcNum"]
if serverNum % replicationFactor != 0:
    print("the number of server %d and replication factor %d does not much", serverNum, replicationFactor)
    exit(1)
mId = 0
for ip in machines:
    dcId = mId % dcNum
    mId = mId + 1
    if dcId not in dc_ip_map:
        dc_ip_map[dcId] = []
    if ip not in dc_ip_map[dcId]:
        dc_ip_map[dcId].append(ip)

machines = config["servers"]["coordMachines"]
mId = 0
for ip in machines:
    dcId = mId % dcNum
    mId = mId + 1
    if dcId not in dc_ip_map:
        dc_ip_map[dcId] = []
    if ip not in dc_ip_map[dcId]:
        dc_ip_map[dcId].append(ip)

machines = config["clients"]["machines"]
mId = 0
for ip in machines:
    dcId = mId % dcNum
    mId = mId + 1
    if dcId not in dc_ip_map:
        dc_ip_map[dcId] = []
    if ip not in dc_ip_map[dcId]:
        dc_ip_map[dcId].append(ip)

machines = config["clients"]["networkMeasureMachines"]
mId = 0
for ip in machines:
    dcId = mId % dcNum
    mId = mId + 1
    if dcId not in dc_ip_map:
        dc_ip_map[dcId] = []
    if ip not in dc_ip_map[dcId]:
        dc_ip_map[dcId].append(ip)

print(dc_ip_map)

dc_delay_map = {}
for dc_id, dst_list in enumerate(config["experiment"]["latency"]["oneWayDelay"]):
    if dc_id not in dc_delay_map:
        dc_delay_map[dc_id] = {}
    for dst_dc_id, dis in enumerate(dst_list):
        if dst_dc_id != dc_id:
            dc_delay_map[dc_id][dst_dc_id] = dis

print(dc_delay_map)

variance = config["experiment"]["latency"]["variance"]
if variance != "off":
    variance = float(variance) / 100.0
distribution = config["experiment"]["latency"]["distribution"]

# cmd prefix
tc = 'sudo tc'

# cmd
clean_cmd = '%s qdisc del dev %s root;' % (tc, dev)

# Sets up delays among different DCs
dc_ip_list = dc_ip_map.keys()
dc_ip_list.sort()
for dc_id in dc_ip_list:
    print("DataCenter: %s" % dc_id)
    ip_list = dc_ip_map[dc_id]
    dst_delay_table = dc_delay_map[dc_id]

    if not dst_delay_table:
        continue

    for ip in ip_list:
        shell_cmd = "ssh %s@%s \"%s" % (user, ip, clean_cmd)
        shell_cmd += "\"" + suffix
        print("Executes: %s" % shell_cmd)
        os.system(shell_cmd)
        print("Done")
        print("")
    print("")
