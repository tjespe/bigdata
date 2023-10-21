# Find the average number of activities per user.

from DbConnector import DbConnector


def avg_number_of_activities_per_user():
    connection = DbConnector()
    client = connection.client
    db = connection.db


    query = db.activities.aggregate([
        {
            "$group": {
               "_id": "$userid",
                "count": {"$sum":1}
            }
        },
        {
            "$group": {
                "_id": None,
                "avgNumActivities": {"$avg":"$count"}
            }
        }
    ])

    result = query[0]["avgNumActivities"]
    if result:
        print("The average number of activities: ")
        print(result)
    else:
        print("Something went wrong")

    client.close()


avg_number_of_activities_per_user()