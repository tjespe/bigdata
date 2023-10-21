# Find the top 20 users who have gained the most altitude meters

from MongoDbConnector import DbConnector


def top_20_users_most_gained_altitude_meters():
    connection = DbConnector()
    client = connection.client
    db = connection.db

    query = db.activities.aggregate(
        [
            {"$unwind": "$trackpoints"},
            {"$match": {"trackpoints.altitude_diff": {"$gt": 0}}},
            {
                "$group": {
                    "_id": "$user_id",
                    "total_alt": {"$sum": "$trackpoints.altitude_diff"},
                }
            },
            {"$sort": {"total_alt": -1}},
            {"$limit": 20},
        ]
    )

    if query:
        print("Top 20 users with the most altitude meters: ")
        for doc in query:
            print(doc)
    else:
        print("Something went wrong")

    client.close()


top_20_users_most_gained_altitude_meters()
