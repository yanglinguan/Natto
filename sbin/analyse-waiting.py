#!/usr/bin/python
import os
import argparse

arg_parser = argparse.ArgumentParser(description="stop exp.")

# Cluster configuration file
arg_parser.add_argument('-c', '--config', dest='config', nargs='?',
                        help='configuration file', required=True)
args = arg_parser.parse_args()


def analyse_waiting():
    txn_map = {}
    lists = os.listdir(args.config)
    for f in lists:
        if f.endswith("_commitOrder.log"):
            lines = open(f, "r").read()
            for line in lines:
                items = line.split(" ")
                txn_id = items[0]
                wait_num = int(items[1])
                if txn_id not in txn_map:
                    txn_map[txn_id] = wait_num
                txn_map[txn_id] = max(txn_map[txn_id], wait_num)

    f = open("waiting.analyse", "w")
    for key, value in txn_map.items():
        f.write(key + " " + str(value) + "\n")


def main():
    analyse_waiting()


if __name__ == "__main__":
    main()
