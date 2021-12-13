#!/usr/bin/python3
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
loss = config["experiment"]["latency"]["packetLoss"]
if variance != "off":
    variance = float(variance) / 100.0
distribution = config["experiment"]["latency"]["distribution"]

# cmd prefix
tc = 'sudo tc'
class_cmd = '%s class add dev %s parent' % (tc, dev)
delay_cmd = '%s qdisc add dev %s handle' % (tc, dev)
filter_cmd = '%s filter add dev %s pref' % (tc, dev)

# cmd
clean_cmd = '%s qdisc del dev %s root;' % (tc, dev)
setup_cmd = '%s qdisc add dev %s root handle 1: htb;' % (tc, dev)
setup_cmd += '%s 1: classid 1:1 htb rate %s;' % (class_cmd, bandwidth)

# Sets up delays among different DCs
dc_ip_list = dc_ip_map.keys()
sorted(dc_ip_list)
for dc_id in dc_ip_list:
    print("DataCenter: %s" % dc_id)
    ip_list = dc_ip_map[dc_id]
    dst_delay_table = dc_delay_map[dc_id]

    if not dst_delay_table:
        continue

    for ip in ip_list:
        shell_cmd = "ssh %s@%s \"%s %s" % (user, ip, clean_cmd, setup_cmd)
        handle = 1
        dst_delay_list = dst_delay_table.keys()
        for dst_dc_id in dst_delay_list:
            delay = dst_delay_table[dst_dc_id]
            if dst_dc_id not in dc_ip_map:
                continue
            dst_ip_list = dc_ip_map[dst_dc_id]

            handle += 1
            shell_cmd += "%s 1:1 classid 1:%d htb rate %s;" % \
                         (class_cmd, handle, bandwidth)
            shell_cmd += "%s %d: parent 1:%d netem delay %s" % \
                         (delay_cmd, handle, handle, delay)
            
            if variance != "off":
                var = str(float(delay[:-2]) * variance) + "ms"
                shell_cmd += " %s distribution %s" % \
                             (var, distribution)
            if loss != "off":
                shell_cmd += " loss %s" % (loss)
            
            shell_cmd += ";"
                
            for dst_ip in dst_ip_list:
                shell_cmd += "%s %d protocol ip u32 match ip dst %s flowid 1:%d;" % \
                             (filter_cmd, handle, dst_ip, handle)
        shell_cmd += "\"" + suffix
        print("Executes: %s" % shell_cmd)
        os.system(shell_cmd)
        print("Done")
        print("")
    print("")
