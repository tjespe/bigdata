

from DbConnector import DbConnector


def num_users_activites_trackpoints():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """ 
            SELECT
             (SELECT COUNT(*) FROM User) AS user_count,
             (SELECT COUNT(*) FROM Activity) AS activity_count,
             (SELECT COUNT(*) FROM TrackPoint) AS trackpoint_count
            """
    
    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        user_count, activity_count, trackpoint_count = result
        print(f"Number of users: {user_count}")
        print(f"Number of activities: {activity_count}")
        print(f"Number of trackpoints: {trackpoint_count}")
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    num_users_activites_trackpoints()