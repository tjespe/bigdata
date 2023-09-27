import numpy as np
from collections import defaultdict
from io import StringIO
import os
from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime, timezone

import pandas as pd

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
                prev_lat DOUBLE,
                lon DOUBLE NOT NULL,
                prev_lon DOUBLE,
                altitude INT,
                altitude_diff INT,
                date_days DOUBLE NOT NULL,
                date_time DATETIME NOT NULL,
                FOREIGN KEY (activity_id) REFERENCES Activity(id)
            )
            """
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
            """
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
        labels_path = os.path.join(os.path.dirname(__file__), "..", "dataset", "labeled_ids.txt")
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

    def insert_activity_and_trackpoint_data(self):
        """
        Add data to Activity and TrackPoint tables based on the files in the dataset (../dataset/Data)
        """

        # Iterate over all subdir names in the dataset
        for subdir_name in os.listdir(DATASET_PATH):
            user_id = int(subdir_name)
            print("Inserting data for user", user_id)

            # Iterate over all files in the Trajectory subdir of the subdir
            # and create Activity objects
            trajectory_path = os.path.join(DATASET_PATH, subdir_name, "Trajectory")
            for activity_file_name in os.listdir(trajectory_path):
                with open(os.path.join(trajectory_path, activity_file_name), "r") as f:
                    lines = f.readlines()
                    # The first 6 lines are irrelevant
                    relevant_lines = lines[6:]
                # If there are more than 2500 trackpoints, we want to exclude the file (as specified in the assignment)
                if len(relevant_lines) > 2500:
                    continue
                # The remaining relevant files can be read as a csv
                df = pd.read_csv(StringIO("".join(relevant_lines)), header=None)
                # These columns correspond to the columns in the csv
                df.columns = ["lat", "lon", "irrelevant", "altitude", "date_days", "date", "time"]
                # Change lat, lon, altitude and date_days to correct types
                for col in ["lat", "lon", "altitude", "date_days"]:
                    df[col] = df[col].astype(float)
                # Some altitude values are missing, so we set them to nan
                df.loc[df["altitude"] == -777, "altitude"].fillna(inplace=True, value=float("nan"))
                # Create Activity object
                start_time = datetime.strptime(df["date"][0] + " " + df["time"][0], "%Y-%m-%d %H:%M:%S")
                end_time = datetime.strptime(df["date"][len(df) - 1] + " " + df["time"][len(df) - 1], "%Y-%m-%d %H:%M:%S")
                # transportation_mode = transportation_modes.get((start_time, end_time), None)
                activity = (user_id, None, start_time, end_time)
                # Insert Activity object into Activity table
                query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"
                self.cursor.execute(query, activity)
                self.db_connection.commit()
                # Get the id of the inserted Activity object
                activity_id = self.cursor.lastrowid
                # Create TrackPoint objects
                trackpoints = []
                prev_row = None
                for _, row in df.iterrows():
                    altitude_diff = None
                    prev_lat = None
                    prev_lon = None
                    if prev_row is not None:
                        if not np.isnan(prev_row["altitude"]):
                            altitude_diff = row["altitude"] - prev_row["altitude"]
                        prev_lat = prev_row["lat"]
                        prev_lon = prev_row["lon"]
                    trackpoint = (activity_id, row["lat"], prev_lat, row["lon"], prev_lon, row["altitude"], altitude_diff, row["date_days"], row["date"] + " " + row["time"])
                    trackpoints.append(trackpoint)
                    prev_row = row
                # Bulk insert TrackPoint objects into TrackPoint table
                query = "INSERT INTO TrackPoint (activity_id, lat, prev_lat, lon, prev_lon, altitude, altitude_diff, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                self.cursor.executemany(query, trackpoints)
                self.db_connection.commit()

    def set_transportation_modes(self):
        # Get users from database that have labels
        query = "SELECT id FROM User WHERE has_labels = 1"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        user_ids = [row[0] for row in rows]
        print("Found %d users with labels" % len(user_ids))
        print(user_ids)

        transportation_mode_instances = defaultdict(int)

        # Iterate over all users with labels
        for user_id in user_ids:
            print("Updating transportation modes for user", user_id)

            # Get transportation modes from subdir_name/labels.txt
            subdir_name = str(user_id).rjust(3, "0")
            labels_path = os.path.join(DATASET_PATH, subdir_name, "labels.txt")
            update_values = []
            with open(labels_path, "r") as f:
                lines = f.readlines()
                for line in lines[1:]:
                    start_time, end_time, transportation_mode = line.split("\t")
                    start_time = datetime.strptime(start_time, "%Y/%m/%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    end_time = datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
                    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
                    transportation_mode = transportation_mode.strip()
                    update_values.append((transportation_mode, user_id, start_time, end_time))
                    transportation_mode_instances[transportation_mode] += 1

            # Bulk update Activity table
            print(update_values)
            query = "UPDATE Activity SET transportation_mode = %s WHERE user_id = %s AND start_date_time = %s AND end_date_time = %s"
            self.cursor.executemany(query, update_values)
            self.db_connection.commit()
        
        print("Set transportation modes for all users")
        print("Transportation mode instances:")
        for key, value in transportation_mode_instances.items():
            print(key, value)


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
    program.drop_all_tables()
    # program.drop_table(table_name="TrackingPoint")
    # program.drop_table(table_name="Activity")
    program.create_tables()
    program.set_indices()
    program.insert_user_data()
    # _ = program.fetch_data(table_name="User")
    program.insert_activity_and_trackpoint_data()
    program.set_transportation_modes()
    # program.show_tables()


if __name__ == '__main__':
    main()
