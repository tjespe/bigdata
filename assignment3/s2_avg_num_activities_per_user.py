# Find the average number of activities per user.

from MongoDbConnector import DbConnector


def avg_number_of_activities_per_user():
    connection = DbConnector()
    client = connection.client
    db = connection.db
    user_count = db["users"].count_documents({})
    activity_count = db["activities"].count_documents({})

    print("The average number of activities per user: ", activity_count / user_count)

    client.close()


avg_number_of_activities_per_user()
