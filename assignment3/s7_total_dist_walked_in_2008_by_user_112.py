from DbConnector import DbConnector
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
        {"$match": {
            "transportation_mode": "walk",
            "$start_date_time": {"$gte": "2008-01-01T00:00:00", "$lt": "2009-01-01T00:00:00"},
            "transportation_mode": "walk"
        }},
        {"$unwind": "$trackpoints"},
        {"$group": {"_id": "$user_id", "total_distance_2008": {"$sum": "$trackpoints.distance"}
                    }}
    ]

    output = db["activities"].aggregate(pipeline)

    result = list(output)

    # Print result
    if result:
        total_distance_2008 = result[0]["total_distance_2008"]
        print(
            f"Total distance walked by user {user_id} in 2008: {total_distance_2008} km")
    else:
        print(f"No data found")

    client.close()


if __name__ == "__main__":
    total_dist_walked_in_2008_by_user_112()
