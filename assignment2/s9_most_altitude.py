from DbConnector import DbConnector
from helpers import print_table

# Find the top 15 users who have gained the most altitude meters.

def get_users_with_most_altitude_gain():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    query = """
            SELECT
                a.user_id,
                SUM(
                    CASE 
                        WHEN t.altitude_diff > 0 THEN altitude_diff
                        ELSE 0
                    END
                ) as alt_gain_sum
            FROM 
                TrackPoint t
            JOIN 
                Activity a ON a.id = t.activity_id
            GROUP BY
                a.user_id
            ORDER BY alt_gain_sum DESC
            LIMIT 15
            """
  
    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["User ID", "Altitude Gain Sum (meters)"]
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    get_users_with_most_altitude_gain()