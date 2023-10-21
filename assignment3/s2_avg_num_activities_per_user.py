# Find the average number of activities per user.

from MongoDbConnector import DbConnector


def avg_number_of_activities_per_user():
    connection = DbConnector()
    client = connection.client
    db = connection.db

    query = db.activities.aggregate(
        [
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            {"$group": {"_id": None, "avgNumActivities": {"$avg": "$count"}}},
        ]
    )

    result = query.next()["avgNumActivities"]
    if result:
        print("The average number of activities: ")
        print(result)
    else:
        print("Something went wrong")

    client.close()


avg_number_of_activities_per_user()
