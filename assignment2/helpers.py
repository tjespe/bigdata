from tabulate import tabulate

def printtable(data, coloumns):
    print(tabulate(data, headers=coloumns, tablefmt='fancy_grid'))