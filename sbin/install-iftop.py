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


def ssh_exec_thread(ssh_client, command):
    stdin, stdout, stderr = ssh_client.exec_command(command)
    print(stdout.read())
    print(stderr.read())


def main():
    threads = []
    machines_server = utils.parse_server_machine(config)
    machines_client = utils.parse_client_machine(config)
    utils.create_ssh_client([machines_server, machines_client])
    install_cmd = "sudo apt install iftop"
    for ip, machine in machines_client.items():
        thread = threading.Thread(
            target=ssh_exec_thread,
            args=(machine.get_ssh_client(), install_cmd)
        )
        threads.append(thread)
        thread.start()
        print(install_cmd + " # at " + ip)

    for ip, machine in machines_server.items():
        thread = threading.Thread(
            target=ssh_exec_thread,
            args=(machine.get_ssh_client(), install_cmd)
        )
        threads.append(thread)
        thread.start()
        print(install_cmd + " # at " + ip)

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
