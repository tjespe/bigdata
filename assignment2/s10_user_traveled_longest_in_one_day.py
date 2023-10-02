from DbConnector import DbConnector
from helpers import print_table

# Find the users that have traveled the longest total distance in one day for each transportation mode

# TODO  convert from feet to Km

def user_traveled_longest_in_one_day():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    for transportation_mode in [
        "walk",
        "taxi",
        "subway",
        "bike",
        "car",
        "bus",
        "airplane",
        "train",
        "boat",
        "run",
    ]:
        query = f""" 
                SELECT Activity.user_id AS user_id, SUM(
                    SQRT(POWER(TrackPoint.lat-TrackPoint.prev_lat, 2) + POWER(TrackPoint.lon-TrackPoint.prev_lon, 2))
                ) AS total_distance
                FROM TrackPoint INNER JOIN Activity ON Activity.id = TrackPoint.activity_id
                WHERE Activity.transportation_mode = '{transportation_mode}'
                GROUP BY user_id, DATE(Activity.start_date_time)
                ORDER BY total_distance DESC
                LIMIT 1;
                """
        
        cursor.execute(query)
        result = cursor.fetchall()

        print(transportation_mode)
        if result:
            columns = ["User ID", "Distance moved"]
            print_table(result, columns)
        else:
            print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    user_traveled_longest_in_one_day()