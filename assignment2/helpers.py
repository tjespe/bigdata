import numpy as np
from tabulate import tabulate

def print_table(data, coloumns):
    print(tabulate(data, headers=coloumns, tablefmt='fancy_grid'))

def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees).
    
    All args must be of equal length.

    Returns distance in meters.

    Source: https://stackoverflow.com/a/29546836/5303541
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6378.137 * c
    m = km * 1000
    return m