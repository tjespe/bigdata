from typing import Set, Tuple
import numpy as np

import pandas as pd
from DbConnector import DbConnector
from helpers import haversine_np, print_table
from haversine import haversine, Unit
from math import ceil
from sklearn.cluster import DBSCAN
from datetime import datetime


# Find the number of users which have been close to each other in time and space.
# Close is defined as the same space (50 meters) and for the same half minute (30 seconds)

POINTS_PER_QUERY = 100_000

def users_close_in_time_and_space():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    # Create a set containing IDs of all users that have met other users
    users_with_meetings = set()

    min_euclidian_radial = 0
    iteration = 0
    start_time = pd.Timestamp.now()
    while True:
        print(
            f"\nIteration {iteration}/{ceil(9.6e6/POINTS_PER_QUERY)}, "
            f"min_euclidian_radial = {min_euclidian_radial}, "
            f"Elapsed time: {(pd.Timestamp.now()-start_time)/pd.Timedelta(seconds=1):.2f} seconds"
        )
        print("Fetching data...", end="\r")
        t = datetime.now()
        query = f"""
            SELECT user_id, activity_id, lat, lon, date_days, altitude, euclidian_radial
            FROM TrackPoint
            LEFT JOIN Activity ON TrackPoint.activity_id = Activity.id
            WHERE euclidian_radial > {min_euclidian_radial}
            ORDER BY euclidian_radial
            LIMIT {POINTS_PER_QUERY}
        """
        cursor.execute(query)
        result = pd.DataFrame(
            cursor.fetchall(),
            columns=["user_id", "activity_id", "lat", "lon", "date_days", "altitude", "euclidian_radial"]
        )
        print("Fetched data. Elapsed time: ", datetime.now()-t, "ms")

        if len(result) == 0:
            break

        num_rows = result.shape[0]

        # Create x and y columns containing meters based on lat and lon
        print("Preprocessing...", end="\r")
        t = datetime.now()
        result["y"] = haversine_np(np.zeros(num_rows), result["lat"], np.zeros(num_rows), np.zeros(num_rows))
        result["y"] = result["y"]*np.sign(result["lat"])
        result["x"] = haversine_np(result["lon"], result["lat"], np.zeros(num_rows), result["lat"])
        result["x"] = result["x"]*np.sign(result["lon"])

        # Scale x, y and time so that the distance limits equal 1
        result["x_scaled"] = result["x"] / 50
        result["y_scaled"] = result["y"] / 50
        result["time_scaled"] = result["date_days"]*24*60*60 / 30
        print("Preprocessing done. Elapsed time: ", datetime.now()-t, "ms")

        # Apply DBSCAN clustering
        print("Applying DBSCAN...", end="\r")
        t = datetime.now()
        epsilon = 1
        db = DBSCAN(eps=epsilon, min_samples=1, metric='l2').fit(
            result[["x_scaled", "y_scaled", "time_scaled"]]
        )
        print("DBSCAN done. Elapsed time: ", datetime.now()-t, "ms")
        labels = db.labels_
        result["cluster"] = labels

        # Discard clusters that only have 1 unique user_id
        print("Discarding irrelevant clusters")
        activities_per_cluster = result.groupby("cluster")["user_id"].nunique()
        relevant_clusters = activities_per_cluster[activities_per_cluster > 1].index
        result = result[result["cluster"].isin(relevant_clusters)]
        
        print("Updating users_with_meetings")
        users_with_meetings.update(set(result["user_id"]))

        # Define the next minimum euclidian radial. We must subtract sqrt(2)
        # because we need some overlap, in case the last trackpoint in this
        # query is close to the first trackpoint in the next query.
        min_euclidian_radial = result["euclidian_radial"].max(skipna=True) - np.sqrt(2)
        if np.isnan(min_euclidian_radial):
            print(min_euclidian_radial,"is nan!")
        iteration += 1

    if users_with_meetings:
        columns = ["Number of distinct close users"]
        print_table(len(users_with_meetings), columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    users_close_in_time_and_space()
