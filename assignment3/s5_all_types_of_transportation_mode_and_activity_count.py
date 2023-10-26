# Find all types of transportation modes and count how many activities that are
# tagged with these transportation mode labels. Do not count the rows where
# the mode is null.

from MongoDbConnector import DbConnector
from helpers import print_table


def all_types_of_transportation_modes_and_activity_count_per_mode():
    connection = DbConnector()
    client = connection.client
    db = connection.db

    query = db.activities.aggregate(
        [
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
    )

    if query:
        print("Number of activities per transportation mode: ")
        rows = []
        while doc := query.try_next():
            rows.append([doc["_id"], doc["count"]])
        columns = ["Transportation mode", "Count"]
        print_table(rows, columns)
    else:
        print("Something went wrong")

    client.close()


all_types_of_transportation_modes_and_activity_count_per_mode()
