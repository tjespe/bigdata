from DbConnector import DbConnector
from helpers import print_table

# Find activities that are registered multiple times. You should find the query even if it gives zero result.

def activities_registered_multiple_times():
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
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    activities_registered_multiple_times()