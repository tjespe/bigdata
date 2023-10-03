from typing import Set, Tuple
import numpy as np

import pandas as pd
from DbConnector import DbConnector
from helpers import haversine_np
from math import ceil
from sklearn.cluster import DBSCAN


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
    page = 0
    start_time = pd.Timestamp.now()
    prev_result = None
    while True:
        print(
            f" Page {page} of (estimated) {ceil(9681756/POINTS_PER_QUERY)}, "
            f"min_euclidian_radial = {min_euclidian_radial}, "
            f"Elapsed time: {(pd.Timestamp.now()-start_time)/pd.Timedelta(seconds=1):.2f} seconds",
            end="\r"
        )
        query = f"""
            SELECT user_id, activity_id, lat, lon, date_days, altitude, euclidian_radial
            FROM TrackPoint
            LEFT JOIN Activity ON TrackPoint.activity_id = Activity.id
            WHERE euclidian_radial > {min_euclidian_radial}
            ORDER BY euclidian_radial
            LIMIT {POINTS_PER_QUERY}
        """
        cursor.execute(query)
        raw_result = pd.DataFrame(
            cursor.fetchall(),
            columns=["user_id", "activity_id", "lat", "lon", "date_days", "altitude", "euclidian_radial"]
        )

        if raw_result.empty:
            print("\Empty result. Stopping.")
            break

        if prev_result is not None and prev_result.shape == raw_result.shape and (prev_result == raw_result).all().all():
            # This can happen because the min_euclidian_radial we set for the next
            # query is defined as the max of this minus sqrt(2), so for the last
            # chunk of data, we are likely to fetch it again and again.
            print("\nResult is the same as previous iteration. Stopping.")
            break

        prev_result = raw_result.copy()

        num_rows = raw_result.shape[0]

        # Create x and y columns containing meters based on lat and lon
        processed_df = raw_result.copy()
        processed_df["y"] = haversine_np(np.zeros(num_rows), processed_df["lat"], np.zeros(num_rows), np.zeros(num_rows))
        processed_df["y"] = processed_df["y"]*np.sign(processed_df["lat"])
        processed_df["x"] = haversine_np(processed_df["lon"], processed_df["lat"], np.zeros(num_rows), processed_df["lat"])
        processed_df["x"] = processed_df["x"]*np.sign(processed_df["lon"])

        # Scale x, y and time so that the distance limits equal 1
        processed_df["x_scaled"] = processed_df["x"] / 50
        processed_df["y_scaled"] = processed_df["y"] / 50
        processed_df["time_scaled"] = processed_df["date_days"]*24*60*60 / 30

        # Apply DBSCAN clustering
        epsilon = 1
        db = DBSCAN(eps=epsilon, min_samples=1, metric='l2').fit(
            processed_df[["x_scaled", "y_scaled", "time_scaled"]]
        )
        labels = db.labels_
        processed_df["cluster"] = labels

        # Discard clusters that only have 1 unique user_id
        activities_per_cluster = processed_df.groupby("cluster")["user_id"].nunique()
        relevant_clusters = activities_per_cluster[activities_per_cluster > 1].index
        filtered_result = processed_df[processed_df["cluster"].isin(relevant_clusters)]
        
        users_with_meetings.update(set(filtered_result["user_id"]))

        # Define the next minimum euclidian radial. We must subtract sqrt(2)
        # because we need some overlap, in case the last trackpoint in this
        # query is close to the first trackpoint in the next query.
        min_euclidian_radial = raw_result["euclidian_radial"].max(skipna=True) - np.sqrt(2)
        if np.isnan(min_euclidian_radial):
            raise Exception("min_euclidian_radial is nan!")
        page += 1

    if users_with_meetings:
        print("Number of distinct close users:", len(users_with_meetings))
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    users_close_in_time_and_space()
