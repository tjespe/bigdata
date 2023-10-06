from DbConnector import DbConnector
from helpers import print_table

# Find the users that have traveled the longest total distance in one day for each transportation mode

def user_traveled_longest_in_one_day():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor
    cursor.execute("SELECT DISTINCT transportation_mode FROM Activity;")
    result = cursor.fetchall()
    transportation_modes = [row[0] for row in result if row[0] is not None]

    for transportation_mode in transportation_modes:
        query = f""" 
            SELECT Activity.user_id AS user_id, SUM(meters_moved) AS total_distance
            FROM TrackPoint INNER JOIN Activity ON Activity.id = TrackPoint.activity_id
            WHERE Activity.transportation_mode = '{transportation_mode}'
            GROUP BY user_id, DATE(TrackPoint.date_time)
            ORDER BY total_distance DESC
            LIMIT 1;
        """
        
        cursor.execute(query)
        result = cursor.fetchall()

        print(transportation_mode)
        if result:
            columns = ["User ID", "Distance moved (m)"]
            print_table(result, columns)
        else:
            print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    user_traveled_longest_in_one_day()