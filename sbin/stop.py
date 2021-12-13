#!/usr/bin/python3
import argparse
import threading

import utils

arg_parser = argparse.ArgumentParser(description="stop exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=True)
args = arg_parser.parse_args()

# Reads configurations
config = utils.load_config(args.config)

turn_on_network_measure = utils.is_network_measure(config)


def ssh_exec_thread(ssh_client, command):
    stdin, stdout, stderr = ssh_client.exec_command(command)
    print(stdout.read())
    print(stderr.read())


def stop_servers(threads, machines_server):
    run_dir = utils.get_run_dir(config)
    for ip, machine in machines_server.items():
        for server_id in machine.ids:
            server_dir = run_dir + "/server-" + str(server_id)
            cmd = "killall -9 carousel-server; cd " + server_dir + "; rm -r raft-*"
            print(cmd + " # at " + ip)
            thread = threading.Thread(target=ssh_exec_thread,
                                      args=(machine.ssh_client, cmd))
            threads.append(thread)
            thread.start()


def stop_clients(threads, machines_client):
    for ip, machine in machines_client.items():
        cmd = "killall -9 client"
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread,
                                  args=(machine.ssh_client, cmd))
        threads.append(thread)
        thread.start()


def stop_networkMeasure(threads, machines_network_measure):
    for ip, machine in machines_network_measure.items():
        cmd = "killall -9 networkMeasure"
        print(cmd + " # at " + ip)
        thread = threading.Thread(target=ssh_exec_thread,
                                  args=(machine.ssh_client, cmd))
        threads.append(thread)
        thread.start()


def main():
    threads = []
    machines_server = utils.parse_server_machine(config)
    machines_client = utils.parse_client_machine(config)
    m_list = [machines_server, machines_client]
    if turn_on_network_measure:
        machines_network_measure = utils.parse_network_measure_machine(config)
        m_list.append(machines_network_measure)
    utils.create_ssh_client(m_list)
    stop_clients(threads, m_list[1])
    if turn_on_network_measure:
        stop_networkMeasure(threads, m_list[2])
    stop_servers(threads, m_list[0])

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
