import numpy as np
from collections import defaultdict
from io import StringIO
import os
from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime, timezone

import pandas as pd

from helpers import haversine_np

# Change this if dataset is located somewhere else
DATASET_PATH = os.path.join(os.path.dirname(__file__), "..", "dataset", "Data")


class CreateDatabase:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS User (
                id INT NOT NULL PRIMARY KEY,
                has_labels boolean
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Activity (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                user_id INT NOT NULL,
                transportation_mode VARCHAR(30),
                start_date_time DATETIME NOT NULL,
                end_date_time DATETIME NOT NULL,
                FOREIGN KEY (user_id) REFERENCES User(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id INT NOT NULL,
                lat DOUBLE NOT NULL,
                lon DOUBLE NOT NULL,
                altitude DOUBLE,
                altitude_diff DOUBLE,
                meters_moved DOUBLE,
                minutes_diff DOUBLE,
                date_days DOUBLE NOT NULL,
                date_time DATETIME NOT NULL,
                euclidian_radial DOUBLE NOT NULL,
                FOREIGN KEY (activity_id) REFERENCES Activity(id)
            )
            """,
        ]

        for query in queries:
            print("Executing", query)
            self.cursor.execute(query)
            self.db_connection.commit()

    def set_indices(self):
        """
        Sets relevant indices on the database.
        """
        queries = [
            """
            ALTER TABLE TrackPoint ADD INDEX idx_trackpoint_activity_date (activity_id, date_days)
            """,
            """
            ALTER TABLE TrackPoint ADD INDEX idx_trackpoint_euclidian_radial (euclidian_radial)
            """,
        ]
        for query in queries:
            print("Executing", query)
            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            except Exception as e:
                print("Failed to create index:", e)

    def insert_user_data(self):
        """
        Add data to User table based on the subdir names in the dataset (../dataset/Data)
        """
        # Get all subdir names in the dataset
        subdir_names = os.listdir(DATASET_PATH)
        print("Found %d users in dataset" % len(subdir_names))
        print(subdir_names)
        user_ids = set([int(subdir_name) for subdir_name in subdir_names])

        # Get list of users that have labels (from dataset/Data/labels.txt)
        labels_path = os.path.join(
            os.path.dirname(__file__), "..", "dataset", "labeled_ids.txt"
        )
        with open(labels_path, "r") as f:
            lines = f.readlines()
            labels = [line.split(" ")[0] for line in lines]
            user_ids_with_labels = set([int(label) for label in labels])
            print("Found %d users with labels" % len(user_ids_with_labels))
            print(user_ids_with_labels)

        # Bulk insert into User table
        query = "INSERT INTO User (id, has_labels) VALUES (%s, %s)"
        values = [(user_id, user_id in user_ids_with_labels) for user_id in user_ids]
        self.cursor.executemany(query, values)
        self.db_connection.commit()

    def create_activities_for_users_using_labels(self):
        # Get users from database that have labels
        query = "SELECT id FROM User WHERE has_labels = 1"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        user_ids = [row[0] for row in rows]
        print("Found %d users with labels" % len(user_ids))
        print(user_ids)

        # Iterate over all users with labels
        for user_id in user_ids:
            print("Creating activities for user", user_id)

            # Get transportation modes from subdir_name/labels.txt
            subdir_name = str(user_id).rjust(3, "0")
            labels_path = os.path.join(DATASET_PATH, subdir_name, "labels.txt")
            activites_to_add = []
            with open(labels_path, "r") as f:
                lines = f.readlines()
                for line in lines[1:]:
                    start_time, end_time, transportation_mode = line.split("\t")
                    activites_to_add.append(
                        (
                            user_id,
                            transportation_mode.strip(),
                            start_time,
                            end_time,
                        )
                    )

            # Bulk update Activity table
            print(activites_to_add)
            query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"
            self.cursor.executemany(query, activites_to_add)
            self.db_connection.commit()

    def insert_activity_and_trackpoint_data(self):
        """
        Add data to Activity and TrackPoint tables based on the files in the dataset (../dataset/Data)
        """
        subdir_names = os.listdir(DATASET_PATH)
        user_ids = [int(subdir_name) for subdir_name in subdir_names]

        self.cursor.execute("SELECT id FROM User WHERE has_labels = 1")
        users_using_labels = [row[0] for row in self.cursor.fetchall()]

        # Iterate over all subdir names in the dataset
        for subdir_name, user_id in sorted(
            zip(subdir_names, user_ids), key=lambda x: x[1]
        ):
            print("Inserting data for user", user_id)

            # Iterate over all files in the Trajectory subdir of the subdir
            # and create Activity objects
            trajectory_path = os.path.join(DATASET_PATH, subdir_name, "Trajectory")
            for activity_file_name in os.listdir(trajectory_path):
                with open(os.path.join(trajectory_path, activity_file_name), "r") as f:
                    lines = f.readlines()
                    # The first 6 lines are irrelevant
                    relevant_lines = lines[6:]
                # The remaining relevant files can be read as a csv
                df: pd.DataFrame = pd.read_csv(
                    StringIO("".join(relevant_lines)), header=None
                )
                # These columns correspond to the columns in the csv
                df.columns = [
                    "lat",
                    "lon",
                    "irrelevant",
                    "altitude",
                    "date_days",
                    "date",
                    "time",
                ]
                # Change lat, lon, altitude and date_days to correct types
                for col in ["lat", "lon", "altitude", "date_days"]:
                    df[col] = df[col].astype(float)
                # Some altitude values are missing, so we set them to nan
                df.loc[df["altitude"] == -777, "altitude"].fillna(
                    inplace=True, value=float("nan")
                )
                df["activity_id"] = np.nan
                if user_id in users_using_labels:
                    query = "SELECT id, start_date_time, end_date_time FROM Activity WHERE user_id = %s"
                    self.cursor.execute(query, (user_id,))
                    user_activities = pd.DataFrame(
                        self.cursor.fetchall(),
                        columns=["id", "start_date_time", "end_date_time"],
                    )
                    user_activities["start_date_time"] = pd.to_datetime(
                        user_activities["start_date_time"]
                    )
                    user_activities["end_date_time"] = pd.to_datetime(
                        user_activities["end_date_time"]
                    )
                    # Find the appropriate activity IDs for every trackpoint
                    for i in range(len(df)):
                        date_time = df["date"][i] + " " + df["time"][i]
                        date_time = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
                        activity_id = user_activities[
                            (user_activities["start_date_time"] <= date_time)
                            & (user_activities["end_date_time"] >= date_time)
                        ]["id"]
                        if activity_id.empty:
                            continue
                        df["activity_id"][i] = activity_id.values[0]
                else:
                    # Create Activity object
                    start_time = datetime.strptime(
                        df["date"][0] + " " + df["time"][0], "%Y-%m-%d %H:%M:%S"
                    )
                    end_time = datetime.strptime(
                        df["date"][len(df) - 1] + " " + df["time"][len(df) - 1],
                        "%Y-%m-%d %H:%M:%S",
                    )
                    # transportation_mode = transportation_modes.get((start_time, end_time), None)
                    activity = (user_id, None, start_time, end_time)
                    # Insert Activity object into Activity table
                    query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"
                    self.cursor.execute(query, activity)
                    self.db_connection.commit()
                    # Get the id of the inserted Activity object, and use as ID for
                    # all TrackPoint objects in this dataframe
                    activity_id = self.cursor.lastrowid
                    df["activity_id"] = activity_id
                # If one activity has more than 2500 trackpoints, we drop it
                activities_to_skip = (
                    df.groupby("activity_id")
                    .filter(lambda x: len(x) > 2500)["activity_id"]
                    .unique()
                )
                df = df[~df["activity_id"].isin(activities_to_skip)]
                if df.empty:
                    continue
                # Calculate Haversine distance moved between (lat, lon) and previous (lat, lon)
                df["prev_lon"] = df["lon"].shift(1)
                df["prev_lat"] = df["lat"].shift(1)
                df["meters_moved"] = haversine_np(
                    df["lon"], df["lat"], df["prev_lon"], df["prev_lat"]
                )
                # Convert altitude from feet to meters
                df["altitude"] = df["altitude"] * 0.3048
                # Add altitude_diff column representing the difference between current altitude and previous altitude
                df["altitude_diff"] = df["altitude"].diff()
                # Add time diff since last trackpoint
                df["minutes_diff"] = df["date_days"].diff() * 24 * 60
                # Calculate a radial distance in a Euclidian space with two dimensions
                # (distance from center of world and date_days) used for querying for
                # users that have been close to each other in time and space.
                # First, we calculate the Haversine distance of each point to the
                # center of the world.
                df["physical_radial"] = haversine_np(
                    df["lon"], df["lat"], np.zeros(df.shape[0]), np.zeros(df.shape[0])
                )
                # Then, we want to also account for altitude differences by using Pythagoras
                df["physical_radial"] = np.sqrt(
                    df["physical_radial"] ** 2 + df["altitude"] ** 2
                )
                # Then, we have to normalize both dimensions to match the scale of the other dimension.
                # When we query for proximity, we look for users that have been less than 50 meters from
                # each other within a time frame of 30 seconds, thus we want 30 seconds to equal 50 meters
                # in our Euclidian space.
                # For simplicity, we want both of these limits to equal a distance of 1 in our Euclidian space,
                # so that all relevant points must be less than sqrt(2) from each other in Euclidian distance.
                df["seconds"] = df["date_days"] * 24 * 60 * 60
                df["time_dimension"] = df["seconds"] / 30
                df["physical_radial"] = df["physical_radial"] / 50
                df["euclidian_radial"] = np.sqrt(
                    df["physical_radial"] ** 2 + df["time_dimension"] ** 2
                )
                # Create TrackPoint objects
                trackpoints = []
                for i, row in df.iterrows():
                    if np.isnan(row["activity_id"]):
                        continue
                    altitude_diff = row["altitude_diff"]
                    if np.isnan(altitude_diff):
                        altitude_diff = None
                    meters_moved = row["meters_moved"]
                    if np.isnan(meters_moved):
                        meters_moved = None
                    minutes_diff = row["minutes_diff"]
                    if np.isnan(minutes_diff):
                        minutes_diff = None
                    trackpoint = (
                        row["activity_id"],
                        row["lat"],
                        row["lon"],
                        row["altitude"],
                        altitude_diff,
                        meters_moved,
                        minutes_diff,
                        row["date_days"],
                        row["date"] + " " + row["time"],
                        row["euclidian_radial"],
                    )
                    trackpoints.append(trackpoint)
                # Bulk insert TrackPoint objects into TrackPoint table
                query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, altitude_diff, meters_moved, minutes_diff, date_days, date_time, euclidian_radial) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                self.cursor.executemany(query, trackpoints)
                self.db_connection.commit()

    def drop_empty_activities(self):
        query = (
            "DELETE FROM Activity WHERE id NOT IN (SELECT activity_id FROM TrackPoint)"
        )
        self.cursor.execute(query)
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def drop_all_tables(self):
        print("Dropping all tables...")
        query = "DROP TABLE User, Activity, TrackPoint"
        self.cursor.execute(query)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = CreateDatabase()
    # program.drop_all_tables()
    # program.drop_table(table_name="TrackingPoint")
    # program.drop_table(table_name="Activity")
    program.create_tables()
    program.set_indices()
    program.insert_user_data()
    program.create_activities_for_users_using_labels()
    # _ = program.fetch_data(table_name="User")
    program.insert_activity_and_trackpoint_data()
    program.drop_empty_activities()
    # program.show_tables()


if __name__ == "__main__":
    main()
