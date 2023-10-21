# Find all users who have registered transportation_mode and their most used
# transportation_mode

from MongoDbConnector import DbConnector


def most_used_transportation_mode_per_user():
    connection = DbConnector()
    client = connection.client
    db = connection.db

    # creating query
    query = db.activities.aggregate(
        [
            {"$match": {"transportation_mode": {"$ne": None}}},
            {
                "$group": {
                    "_id": {
                        "userid": "$user_id",
                        "transportation_mode": "$transportation_mode",
                    },
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"count": -1}},
            {
                "$group": {
                    "_id": "$id.userid",
                    "transportation_mode": {"$first": "$_id.transportation_mode"},
                }
            },
        ]
    )

    print(query.next())
