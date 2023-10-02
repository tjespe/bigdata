from DbConnector import DbConnector
from helpers import print_table
from haversine import haversine, Unit

# Find the number of users which have been close to each other in time and space.
# Close is defined as the same space (50 meters) and for the same half minute (30 seconds)

def users_close_in_time_and_space():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    #NEED TO SAY HOW WE INTERPRET THIS QUESTION - Not working currently, ALTER TABLE TrackPoint ENGINE=InnoDB;
    #AND haversine(track1.lat,track1.lon,track2.lat,track2.lon, unit='m') <= 50
    query = """
            SELECT COUNT(DISTINCT act1.user_id)
            FROM Activity act1
            INNER JOIN TrackPoint track1 ON act1.id = track1.activity_id
            INNER JOIN Activity act2 ON act1.user_id != act2.user_id
            INNER JOIN TrackPoint track2 ON act2.id = track2.activity_id 
            WHERE (ABS(TIMESTAMPDIFF(SECOND, track1.date_time, track2.date_time)) <= 30 
                AND ABS(track1.altitude -track2.altitude) <= 50) 

            """

    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["Number of distinct close users"]
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    users_close_in_time_and_space()


# """
#             SELECT COUNT(DISTINCT user1.id)
#             FROM User user1
#             INNER JOIN Activity act1 ON user1.id = act1.user_id
#             INNER JOIN TrackPoint track1 ON act1.id = track1.activity_id
#             INNER JOIN User user2 ON user2.id != user1.id
#             INNER JOIN Activity act2 ON user2.id = act2.user_id
#             INNER JOIN TrackPoint track2 ON act2.id = track2.activity_id AND haversine(track1.lat,track1.lon,track2.lat,track2.lon, unit='m')
#             WHERE (
#                 ABS(TIMESTAMPDIFF(SECOND, track1.date_time, track2.date_time)) <= 30
#             )

#             """