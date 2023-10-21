# Find the top 20 users with the highest number of activities. 

from DbConnector import DbConnector


def top_20_users_with_most_activities():
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
            "$sort": { 
                {"count": -1}
            }
        },
        {
            "$limit" : 20
        }

    ])

    if query:
        print("Top 20 users with the most activities: ")
        print(query)
    else:
        print("Something went wrong")


top_20_users_with_most_activities()