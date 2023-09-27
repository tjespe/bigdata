from tabulate import tabulate

def print_table(data, coloumns):
    print(tabulate(data, headers=coloumns, tablefmt='fancy_grid'))