# Find all types of transportation modes and count how many activities that are
# tagged with these transportation mode labels. Do not count the rows where
# the mode is null.

from DbConnector import DbConnector


def tall_types_of_transportation_modes_and_activity_count_per_mode():
    connection = DbConnector()
    client = connection.client
    db = connection.db


    query = db.activities.aggregate([
        {
            "$group": {
               "_id": "$transportation_mode",
                "count": {"$sum":1}
            }
        },
        {
            "$sort": { 
                {"count": -1}
            }
        },

    ])

    ## check to remove the empty acivity mode
    if query:
        print("Top 20 users with the most activities: ")
        print(query)
    else:
        print("Something went wrong")


tall_types_of_transportation_modes_and_activity_count_per_mode()