import numpy as np
from tabulate import tabulate


def print_table(data, coloumns):
    """
    Prints out the specified data in a pretty format, with specified coloumn headers
    """
    print(tabulate(data, headers=coloumns, tablefmt="fancy_grid"))
