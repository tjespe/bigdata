from MongoDbConnector import DbConnector
from helpers import print_table

# Find all users who have taken a taxi.


def users_that_have_taken_taxi():
    # Connect to db
    connection = DbConnector()
    client = connection.client
    db = connection.db

    # Query
    pipeline = [
        {"$match": {"transportation_mode": "taxi"}},
        {"$group": {"_id": "$user_id"}},
    ]

    output = db["activities"].aggregate(pipeline)
    users = [[user["_id"]] for user in output]

    if users:
        columns = ["User ID"]
        print_table(users, columns)
    else:
        print("No data found")

    client.close()


if __name__ == "__main__":
    users_that_have_taken_taxi()
