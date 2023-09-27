from DbConnector import DbConnector

# Find the top 15 users with the highest number of activities

def top_15_users_by_higest_num_activities():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """ 
            SELECT activity.id AS user_id, COUNT(activity.id) AS activity_count
            


            SELECT AVG(COUNT(TrackPoint.id)) AS Average, MAX(COUNT(TrackPoint.id)) AS Maximum, MIN(COUNT(TrackPoint.id)) as Minimum
            FROM User INNER JOIN Activity ON User.id = Activity.user_id INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
            GROUP BY User.id

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
    top_15_users_by_higest_num_activities()