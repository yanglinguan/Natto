#!/usr/bin/python3

import argparse
import threading

import utils

arg_parser = argparse.ArgumentParser(description="tcp exp.")

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
    run_dir = utils.get_run_dir(config)
    leader_ips = ["10.0.3.1",
                  "10.0.3.7",
                  "10.0.3.10",
                  "10.0.3.13",
                  "10.0.2.10"]
    machines_server = utils.parse_server_machine(config)
    utils.create_ssh_client(machines_server)
    for sip, server in machines_server.items():
        if sip not in leader_ips:
            continue
        cmd = "iperf3 -s &"
        server.ssh_client.exec_command(cmd)
        threads = []
        for cip, client in machines_server.items():
            if cip == sip or cip not in leader_ips:
                continue
            log_file = run_dir + "tcp-" + sip + "-" + cip + ".log"
            cmd = "iperf3 -c " + sip + " -t 250 -i /tmp > " + log_file
            thread = threading.Thread(
                target=ssh_exec_thread,
                args=(client.ssh_client, cmd),
            )
            threads.append(thread)
            thread.start()
        for t in threads:
            t.join()

        server.ssh_client.exec_command("killall -9 iperf3")


if __name__ == "__main__":
    main()
