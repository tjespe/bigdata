from DbConnector import DbConnector
from helpers import print_table

# Find the users who have tracked an activity in the Forbidden City of Beijing.
# In this question you can consider the Forbidden City to have coordinates that correspond to: lat 39.916, lon 116.397


def users_who_have_tracked_an_activity_in_the_forbidden_city():
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
    users_who_have_tracked_an_activity_in_the_forbidden_city()
