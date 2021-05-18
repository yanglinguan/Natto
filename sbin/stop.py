#!/usr/bin/python
import argparse
import json
import threading

import utils

from paramiko import SSHClient, AutoAddPolicy

arg_parser = argparse.ArgumentParser(description="stop exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=True)
args = arg_parser.parse_args()

# Reads configurations
config = utils.load_config(args.config)

turn_on_network_measure = utils.is_network_measure(config)

machines_client = utils.parse_client_machine(config)

machines_server = utils.parse_server_machine(config)

machines_network_measure = utils.parse_network_measure_machine(config)

run_dir = utils.get_run_dir(config)


def ssh_exec_thread(ssh_client, command):
    stdin, stdout, stderr = ssh_client.exec_command(command)
    print(stdout.read())
    print(stderr.read())


def stop_servers(threads):
    for ip, machine in machines_server.items():
        for server_id in machine.ids:
            server_dir = run_dir + "/server-" + str(server_id)
            cmd = "killall -9 carousel-server; cd " + server_dir + "; rm -r raft-*"
            print(cmd + " # at " + ip)
            thread = threading.Thread(target=ssh_exec_thread,
                                      args=(machine.ssh_client, cmd))
            threads.append(thread)
            thread.start()


def stop_clients(threads):
    for ip, machine in machines_client.items():
        cmd = "killall -9 client"
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread,
                                  args=(machine.ssh_client, cmd))
        threads.append(thread)
        thread.start()


def stop_networkMeasure(threads):
    for ip, machine in machines_network_measure.items():
        cmd = "killall -9 networkMeasure"
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread,
                                  args=(machine.ssh_client, cmd))
        threads.append(thread)
        thread.start()


def main():
    threads = []
    stop_clients(threads)
    if turn_on_network_measure:
        stop_networkMeasure(threads)
    stop_servers(threads)

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
