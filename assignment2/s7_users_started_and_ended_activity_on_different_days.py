from DbConnector import DbConnector
from helpers import print_table

# Find the number of users that have started an activity in one day and ended the activity the next day.
# List the transportation mode, user id and duration for these activities.


def users_started_and_ended_activity_on_different_days():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """  
            SELECT user_id, transportation_mode, start_date_time, end_date_time
            FROM Activity
            GROUP BY user_id, transportation_mode, start_date_time, end_date_time 
            HAVING COUNT(*) > 1;
            """
    
    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["User ID", "Transportation Mode", "Start DateTime", "End Date Time"]
        result = [(row["user_id", "transportation_mode", "start_date_time", "end_date_time"]) for row in result]
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    users_started_and_ended_activity_on_different_days()