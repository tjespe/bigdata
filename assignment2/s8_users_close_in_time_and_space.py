from DbConnector import DbConnector
from helpers import print_table

# Find the number of users which have been close to each other in time and space.
# Close is defined as the same space (50 meters) and for the same half minute (30 seconds)

def s8_users_close_in_time_and_space():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    #NEED TO SAY HOW WE INTERPRET THIS QUESTION - Not working currently
    query = """
            ALTER TABLE TrackPoint ENGINE=InnoDB;
            SELECT COUNT(DISTINCT user1.id)
            FROM User user1
            INNER JOIN Activity act1 ON user1.id = act1.user_id
            INNER JOIN TrackPoint track1 ON act1.id = track1.activity_id
            INNER JOIN User user2 ON user2.id != user1.id
            INNER JOIN Activity act2 ON user2.id = act2.user_id
            INNER JOIN TrackPoint track2 ON act2.id = track2.activity_id
            WHERE (
                ABS(TIMESTAMPDIFF(SECOND, track1.date_time, track2.date_time)) <= 30
            )

            """
    cursor.execute(query)
    coloumns = ["Number of distinct close users"]
    data = cursor.fetchall()
    print_table(data, coloumns)
    db_connection.close()
    connection.close_connection()

s8_users_close_in_time_and_space()