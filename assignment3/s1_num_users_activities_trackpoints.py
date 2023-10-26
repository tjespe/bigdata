from MongoDbConnector import DbConnector

# How many users, activities and trackpoints are there in the dataset?


def num_users_activites_trackpoints():
    # Connect to db
    connection = DbConnector()
    client = connection.client
    db = connection.db

    # Query
    activity_count = db["activities"].count_documents({})
    user_count = len(db["users"].distinct("_id"))

    pipeline = [
        {"$project": {"trackpoint_count": {"$size": "$trackpoints"}}},
        {"$group": {"_id": None, "trackpoint_count": {"$sum": "$trackpoint_count"}}},
    ]
    trackpoint_count = db["activities"].aggregate(pipeline).next()["trackpoint_count"]

    # Print
    print(f"Number of users: {user_count}")
    print(f"Number of activities: {activity_count}")
    print(f"Number of trackpoints: {trackpoint_count}")

    client.close()


if __name__ == "__main__":
    num_users_activites_trackpoints()
