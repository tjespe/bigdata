from DbConnector import DbConnector
from helpers import print_table

# Find the top 15 users with the highest number of activities

def top_15_users_by_higest_num_activities():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """ 
            SELECT * 
            FROM TrackPoint
            LIMIT 10
            """
    
    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["id", "activity_id", "lat", "prev_lat", "lon", "prev_lon", "altitude", "altitude_diff", "date_days", "date_time"]
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    top_15_users_by_higest_num_activities()