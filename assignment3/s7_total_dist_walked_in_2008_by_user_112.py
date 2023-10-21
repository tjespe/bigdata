import datetime
from MongoDbConnector import DbConnector
from helpers import print_table

# Find the total distance (in km) walked in 2008, by user with id=112


def total_dist_walked_in_2008_by_user_112():
    # Connect to db
    connection = DbConnector()
    client = connection.client
    db = connection.db

    # Query
    user_id = 112

    pipeline = [
        {
            "$match": {
                "transportation_mode": "walk",
                "start_time": {
                    "$gte": datetime.datetime(2008, 1, 1),
                    "$lt": datetime.datetime(2009, 1, 1),
                },
                "user_id": user_id,
            }
        },
        {"$unwind": "$trackpoints"},
        {
            "$group": {
                "_id": None,
                "total_distance_2008": {"$sum": "$trackpoints.meters_moved"},
            }
        },
    ]

    output = db["Activity"].aggregate(pipeline)

    result = output.try_next()

    # Print result
    if result:
        total_distance_2008 = result["total_distance_2008"]
        print(
            f"Total distance walked by user {user_id} in 2008: {total_distance_2008} km"
        )
    else:
        print(f"No data found")

    client.close()


if __name__ == "__main__":
    total_dist_walked_in_2008_by_user_112()
