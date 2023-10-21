# Find the top 20 users who have gained the most altitude meters

from DbConnector import DbConnector

def top_20_users_most_gained_altitude_meters():
    connection = DbConnector()
    client = connection.client
    db = connection.db


    query = db.activities.aggregate([
        
        {
            "$group": {
                "_id": "$userid",

            }
        },
        {"$unwind": "$trackpoints"},
    ])

    if query:
        print("Top 20 users with the most gained altitude meters: ")
        print(query)
    else:
        print("Something went wrong")

    client.close()