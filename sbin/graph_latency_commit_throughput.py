#!/usr/bin/python
import argparse
import json
import os
import matplotlib.pyplot as plt

arg_parser = argparse.ArgumentParser(description="graph.")

# Cluster configuration file
arg_parser.add_argument('-d', '--data', dest='data', nargs='?',
                        help='data file', required=False)

args = arg_parser.parse_args()

path = os.getcwd()
if args.data is not None:
    path = args.data

style = {
    "gts": {"color": "red", "line_style": "solid", "marker": 'o'},
    "occ": {"color": "green", "line_style": "dashed", "marker": 'd'}
}


def draw():
    lists = os.listdir(path)
    graph = {}
    lists.sort(key=lambda x: int(os.path.splitext(x)[0].split("-")[1]))
    for f in lists:
        if f.endswith("result"):
            fl = open(os.path.join(path, f), "r")
            data = json.load(fl)
            items = f.split(".")
            protocol = items[0].split("-")[0]
            if protocol not in graph:
                graph[protocol] = {"x": [], "y": []}
            graph[protocol]["x"].append(data["throughput"])
            graph[protocol]["y"].append(data["avg"])

    print(graph)

    for label, data in graph.items():
        x = data["x"]
        y = data["y"]

        plt.plot(x, y, color=style[label]["color"], linestyle=style[label]["line_style"], linewidth=3,
                 marker=style[label]["marker"], markersize=6, label=label)
    plt.xlabel('Commit Throughput (txn/s)')
    plt.ylabel('Average Latency (ms)')
    plt.xlim(0, 500)
    plt.ylim(0, 500)
    plt.legend()
    plt.savefig(os.path.basename(path) + "-latency-commit-throughput.pdf")
    plt.show()


if __name__ == "__main__":
    draw()
