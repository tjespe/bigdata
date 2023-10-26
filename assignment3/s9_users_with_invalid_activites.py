from MongoDbConnector import DbConnector
from helpers import print_table

# Find all users with invalid activities, and the number of invalid activities per user
# An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.


def users_with_invalid_activites():
    # Connect to db
    connection = DbConnector()
    client = connection.client
    db = connection.db

    # Query
    pipeline = [
        {"$unwind": "$trackpoints"},
        {"$match": {"trackpoints.minutes_diff": {"$gte": 5}}},
        # Count unique activity IDs per user
        {
            "$group": {
                "_id": "$user_id",
                "num_invalid_activites": {"$addToSet": "$_id"},
            }
        },
        # Count number of unique activity IDs per user
        {
            "$project": {
                "_id": 0,
                "user_id": "$_id",
                "num_invalid_activites": {"$size": "$num_invalid_activites"},
            }
        },
        {"$sort": {"num_invalid_activites": -1}},
    ]

    output = db["activities"].aggregate(pipeline)
    users = [[user["user_id"], user["num_invalid_activites"]] for user in output]

    # Print result
    if users:
        columns = ["User ID", "Number of Invalid Activites"]
        print(len(users), "users have invalid activities")
        print_table(users, columns)
    else:
        print("No data found")

    client.close()


if __name__ == "__main__":
    users_with_invalid_activites()
