# Find the top 20 users with the highest number of activities.

from MongoDbConnector import DbConnector
from helpers import print_table


def top_20_users_with_most_activities():
    connection = DbConnector()
    client = connection.client
    db = connection.db

    query = db.activities.aggregate(
        [
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20},
        ]
    )

    data = [[user["_id"], user["count"]] for user in query]

    if query:
        print("Top 20 users with the most activities: ")
        columns = ["User ID", "Number of activities"]
        print_table(data, columns)
    else:
        print("Something went wrong")

    client.close()


top_20_users_with_most_activities()
