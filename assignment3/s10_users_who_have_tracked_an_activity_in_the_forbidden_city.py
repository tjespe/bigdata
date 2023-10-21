from MongoDbConnector import DbConnector
from helpers import print_table

# Find the users who have tracked an activity in the Forbidden City of Beijing.
# In this question you can consider the Forbidden City to have coordinates that correspond to: lat 39.916, lon 116.397


def users_who_have_tracked_an_activity_in_the_forbidden_city():
    # Connect to db
    connection = DbConnector()
    client = connection.client
    db = connection.db

    # Query
    coordinates_of_the_forbidden_city = {"lat": 39.916, "lon": 116.397}

    pipeline = [
        {"$unwind": "$trackpoints"},
        {
            "$match": {
                "trackpoints.lat": coordinates_of_the_forbidden_city["lat"],
                "trackpoints.lon": coordinates_of_the_forbidden_city["lon"],
            }
        },
        {"$group": {"_id": "$user_id"}},
    ]

    output = db.activities.aggregate(pipeline)

    users = [[user["_id"]] for user in output]

    # Print result
    if users:
        print("Users who have tracked an activity in the Forbidden City:")
        columns = ["User ID"]
        print_table(users, columns)
    else:
        print("No data found ")

    client.close()


if __name__ == "__main__":
    users_who_have_tracked_an_activity_in_the_forbidden_city()
