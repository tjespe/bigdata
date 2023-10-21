# Find the top 20 users who have gained the most altitude meters

from MongoDbConnector import DbConnector


def top_20_users_most_gained_altitude_meters():
    connection = DbConnector()
    client = connection.client
    db = connection.db

    cursor = db.activities.aggregate(
        [
            {
                "$group": {
                    "_id": "$user_id",
                }
            },
            {"$unwind": "$trackpoints"},
        ]
    )
    result = []
    for document in cursor:
        result.append(document["_id"])
    if result:
        print("Top 20 users with the most gained altitude meters: ")
        print(result)
    else:
        print("Something went wrong")

    client.close()


top_20_users_most_gained_altitude_meters()
