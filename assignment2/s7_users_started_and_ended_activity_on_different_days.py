from DbConnector import DbConnector
from helpers import print_table

# Find the number of users that have started an activity in one day and ended the activity the next day.
# List the transportation mode, user id and duration for these activities.

def users_started_and_ended_activity_on_different_days():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query_count_users = """
                        SELECT COUNT(DISTINCT user_id)
                        FROM Activity
                        WHERE DATEDIFF(end_date_time, start_date_time) = 1;
                        """
    
    cursor.execute(query_count_users)
    user_count = cursor.fetchone()


    query_activities =  """  
                        SELECT user_id, transportation_mode, TIMEDIFF(end_date_time, start_date_time) AS duration, start_date_time, end_date_time
                        FROM Activity
                        WHERE DATEDIFF(end_date_time, start_date_time) = 1;
                        """
    
    cursor.execute(query_activities)
    result = cursor.fetchall()

    if result:
        print(f"Number of users: {user_count[0]}")
        columns = ["User ID", "Transportation Mode", "Duration", "Start Date Time", "End Date Time"]
        print_table(result, columns)
        print(f"Number of users: {user_count[0]}")

    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    users_started_and_ended_activity_on_different_days()