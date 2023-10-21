import bisect
from datetime import datetime
from io import StringIO
import os
from pprint import pprint
import numpy as np

import pandas as pd
from MongoDbConnector import DbConnector
from helpers import haversine_np


# Change this if dataset is located somewhere else
DATASET_PATH = os.path.join(os.path.dirname(__file__), "..", "dataset", "Data")


class DocumentInserter:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_collection(self, collection_name):
        collection = self.db.create_collection(collection_name, check_exists=False)
        print("Created collection: ", collection)

    def insert_activity_and_trackpoint_data(self):
        """
        Add data to Activity and TrackPoint tables based on the files in the dataset (../dataset/Data)
        """

        subdir_names = os.listdir(DATASET_PATH)
        user_ids = [int(subdir_name) for subdir_name in subdir_names]

        # Get users that have labels from the labels file
        labels_path = os.path.join(
            os.path.dirname(__file__), "..", "dataset", "labeled_ids.txt"
        )
        with open(labels_path, "r") as f:
            lines = f.readlines()
            labels = [line.split(" ")[0] for line in lines]
            user_ids_with_labels = set([int(label) for label in labels])
            print("Found %d users with labels" % len(user_ids_with_labels))
            print(user_ids_with_labels)

        # Iterate over all subdir names in the dataset
        for subdir_name, user_id in sorted(
            zip(subdir_names, user_ids), key=lambda x: x[1]
        ):
            print("Inserting data for user", user_id)
            activities = []

            # For users using labels, create activities based on the labels file
            if user_id in user_ids_with_labels:
                print("Creating activities for user", user_id)
                # Get transportation modes from subdir_name/labels.txt
                labels_path = os.path.join(DATASET_PATH, subdir_name, "labels.txt")
                with open(labels_path, "r") as f:
                    lines = f.readlines()
                    for line in lines[1:]:
                        start_time, end_time, transportation_mode = line.split("\t")
                        activities.append(
                            {
                                "transportation_mode": transportation_mode.strip(),
                                "start_time": start_time,
                                "end_time": end_time,
                            }
                        )

            activities_df = pd.DataFrame(
                activities,
                columns=["transportation_mode", "start_time", "end_time"],
            )
            activities_df["start_time"] = pd.to_datetime(activities_df["start_time"])
            activities_df["end_time"] = pd.to_datetime(activities_df["end_time"])
            # Sort activities_df by start_time
            activities_df = activities_df.sort_values(by="start_time").reset_index()

            trackpoints_df = None
            activities_to_add = []

            # Iterate over all files in the Trajectory subdir of the subdir
            # and create Activity objects
            trajectory_path = os.path.join(DATASET_PATH, subdir_name, "Trajectory")
            for file_idx, activity_file_name in enumerate(os.listdir(trajectory_path)):
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
                df["activity_idx"] = np.nan
                # Change lat, lon, altitude and date_days to correct types
                for col in ["lat", "lon", "altitude", "date_days"]:
                    df[col] = df[col].astype(float)
                # Some altitude values are missing, so we set them to nan
                df.loc[df["altitude"] == -777, "altitude"].fillna(
                    inplace=True, value=float("nan")
                )
                # Convert altitude from feet to meters
                df["altitude"] = df["altitude"] * 0.3048
                # Add altitude_diff column representing the difference between current altitude and previous altitude
                df["altitude_diff"] = df["altitude"].diff()
                # Create date_time column
                df["date_time"] = df["date"] + " " + df["time"]
                df["date_time"] = pd.to_datetime(df["date_time"])
                # Add time diff since last trackpoint
                df["minutes_diff"] = df["date_days"].diff() * 24 * 60
                if user_id in user_ids_with_labels:
                    # Create a list of start_times for binary search
                    start_times = activities_df["start_time"].tolist()

                    def find_activity_idx(date_time):
                        # Find the rightmost value less than or equal to date_time
                        idx = bisect.bisect_left(start_times, date_time)

                        # Adjust idx if the found interval doesn't match
                        if (
                            idx == len(activities_df)
                            or activities_df.at[idx, "end_time"] < date_time
                        ):
                            return None
                        return activities_df.at[idx, "index"]

                    df["activity_idx"] = df["date_time"].apply(find_activity_idx)
                    # Drop trackpoints that are not part of an activity
                    df.dropna(subset=["activity_idx"], inplace=True)
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
                    activities_to_add.append(
                        {
                            "user_id": user_id,
                            "transportation_mode": None,
                            "start_time": start_time,
                            "end_time": end_time,
                            "activity_idx": file_idx,
                        }
                    )
                    df["activity_idx"] = file_idx
                if df.empty:
                    continue
                if trackpoints_df is None:
                    trackpoints_df = df
                else:
                    trackpoints_df = pd.concat([trackpoints_df, df])

            if activities_to_add:
                activities_df = pd.DataFrame(
                    activities_to_add,
                    columns=[
                        "user_id",
                        "transportation_mode",
                        "start_time",
                        "end_time",
                        "activity_idx",
                    ],
                ).set_index("activity_idx")

            if trackpoints_df is None:
                print("No trackpoints found for user", user_id)
                continue

            # If one activity has more than 2500 trackpoints, we drop it
            activities_to_skip = (
                trackpoints_df.groupby("activity_idx")
                .filter(lambda x: len(x) > 2500)["activity_idx"]
                .unique()
            )
            trackpoints_df = trackpoints_df[
                ~trackpoints_df["activity_idx"].isin(activities_to_skip)
            ]
            # Calculate Haversine distance moved between (lat, lon) and previous (lat, lon)
            trackpoints_df.sort_values(by=["activity_idx", "date_time"], inplace=True)
            trackpoints_df["prev_lon"] = trackpoints_df.groupby("activity_idx")[
                "lon"
            ].shift(1)
            trackpoints_df["prev_lat"] = trackpoints_df.groupby("activity_idx")[
                "lat"
            ].shift(1)
            trackpoints_df["meters_moved"] = haversine_np(
                trackpoints_df["lon"],
                trackpoints_df["lat"],
                trackpoints_df["prev_lon"],
                trackpoints_df["prev_lat"],
            )

            # Create activity documents
            print("Creating activity documents")
            activities = []
            if trackpoints_df.empty:
                print("No trackpoints found for user", user_id, "after filtering")
                continue
            for activity_idx, activity in activities_df.iterrows():
                activities.append(
                    {
                        "user_id": user_id,
                        "transportation_mode": activity["transportation_mode"],
                        "start_time": activity["start_time"],
                        "end_time": activity["end_time"],
                        "trackpoints": trackpoints_df.loc[
                            trackpoints_df["activity_idx"] == activity_idx,
                            [
                                "lat",
                                "lon",
                                "altitude",
                                "date_days",
                                "date_time",
                                "altitude_diff",
                                "minutes_diff",
                                "meters_moved",
                            ],
                        ].to_dict("records"),
                    }
                )

            # Insert activities
            collection = self.db["activities"]
            print("Inserting", len(activities), "activities for user", user_id)
            chunk_size = 100
            for i in range(0, len(activities), chunk_size):
                collection.insert_many(activities[i : i + chunk_size])

    def count_documents(self, collection_name):
        collection = self.db[collection_name]
        print("Number of documents in collection:", collection.count_documents({}))

    def fetch_sample(self, collection_name):
        collection = self.db[collection_name]
        pprint(collection.find_one())

    def drop_collection(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()


def main():
    program = DocumentInserter()
    program.drop_collection(collection_name="activities")
    program.create_collection(collection_name="activities")
    program.insert_activity_and_trackpoint_data()
    program.count_documents(collection_name="activities")
    # program.fetch_sample(collection_name="activities")


if __name__ == "__main__":
    main()
