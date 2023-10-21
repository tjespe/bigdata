# Find all types of transportation modes and count how many activities that are
# tagged with these transportation mode labels. Do not count the rows where
# the mode is null.

from MongoDbConnector import DbConnector


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
        while doc := query.try_next():
            print(doc)
    else:
        print("Something went wrong")

    client.close()


all_types_of_transportation_modes_and_activity_count_per_mode()
