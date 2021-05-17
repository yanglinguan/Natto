import json
import os

src_path = "$HOME/Projects/go/src/Carousel-GTS/"
server_path = src_path + "carousel-server/"
client_path = src_path + "benchmark/client/"
rpc_path = src_path + "rpc/"
tool_path = src_path + "tools/"
check_server_status_path = tool_path + "checkServerStatus/"
network_measure_path = src_path + "networkMeasure/"

binPath = "/home/l69yang/Projects/go/bin/"


def load_config(config_file_name):
    config_file = open(config_file_name, "r")
    config = json.load(config_file)
    config_file.close()
    return config


def is_network_measure(config):
    dynamic_latency = config["experiment"]["dynamicLatency"]["mode"]
    use_timestamp = config["experiment"]["networkTimestamp"]
    return dynamic_latency and use_timestamp


def get_ssh_username(config):
    return config["experiment"]["ssh"]["username"]


def get_run_dir(config):
    path = os.getcwd()
    if "runDir" in config["experiment"] and len(config["experiment"]["runDir"]) != 0:
        path = config["experiment"]["runDir"]
    return path


class Machine:
    def __init__(self, machine_ip):
        self.machine_ip = machine_ip
        self.ids = []

    def add_id(self, server_id):
        self.ids.append(server_id)


def parse_network_measure_machine(config):
    machines_network_measure = {}
    machines = config["clients"]["networkMeasureMachines_pub"]
    dcId = 0
    for ip in machines:
        machines_network_measure[ip] = Machine(ip)
        machines_network_measure[ip].add_id(str(dcId))
        dcId += 1
    return machines_network_measure


def parse_client_machine(config):
    machines_client = {}
    client_nums = config["clients"]["nums"]
    machines = config["clients"]["machines_pub"]
    for ip in machines:
        machines_client[ip] = Machine(ip)
    for clientId in range(client_nums):
        idx = clientId % len(machines)
        ip = machines[idx]
        machines_client[ip].add_id(str(clientId))
    return machines_client


def parse_server_machine(config):
    machines_server = {}
    server_nums = config["servers"]["nums"]
    machines = config["servers"]["machines_pub"]
    for ip in machines:
        machines_server[ip] = Machine(ip)
    for server_id in range(server_nums):
        idx = server_id % len(machines)
        ip = machines[idx]
        machines_server[ip].add_id(str(server_id))
    return machines_server
