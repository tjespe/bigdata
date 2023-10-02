from DbConnector import DbConnector
from helpers import print_table

# Find all users with invalid activities, and the number of invalid activities per user 
# An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.

def users_with_invalid_activites():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """ 
        SELECT user_id, COUNT(DISTINCT Activity.id) as num_invalid_activites
        FROM Activity LEFT JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
        WHERE TrackPoint.minutes_diff >= 5
        GROUP BY user_id
        ORDER BY num_invalid_activites DESC
    """
    
    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["User ID", "Number of Invalid Activites"]
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    users_with_invalid_activites()