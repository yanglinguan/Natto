import argparse
import os
import numpy as np
import matplotlib.pyplot as plt

arg_parser = argparse.ArgumentParser(description="graph.")

# Cluster configuration file
arg_parser.add_argument('-d', '--data', dest='data', nargs='?',
                        help='data file', required=False)

args = arg_parser.parse_args()

path = os.getcwd()
if args.data is not None:
    path = args.data


def draw():
    x = []
    with open(os.path.join(path, "tmp"), "r") as f:
        x = [float(l) for l in f.readlines()]
    y = np.arange(len(x)) / len(x)
    plt.plot(x, y)
    plt.show()


if __name__ == "__main__":
    draw()
