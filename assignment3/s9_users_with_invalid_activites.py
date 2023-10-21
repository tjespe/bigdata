from DbConnector import DbConnector
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
        {"match": {"$trackpoints.minutes_diff": {"$gte": 5}}},
        {"$group": {"_id": "$user_id", "num_invalid_activities": {"$sum": 1}}},
        {"$sort": {"num_invalid_activities": -1}},
    ]

    output = db["activities"].aggregate(pipeline)

    users = [user["_id"] for user in output]

    # Print result
    if users:
        columns = ["User ID", "Number of Invalid Activites"]
        print_table(users, columns)
    else:
        print("No data found")

    client.close()


if __name__ == "__main__":
    users_with_invalid_activites()
