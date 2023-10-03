

from project.assignment2.DbConnector import DbConnector


def num_users_activites_trackpoints():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """ 
            SELECT
             (SELECT COUNT(*) FROM User) AS user_count,
             (SELECT COUNT(*) FROM Activities) AS activity_count,
             (SELECT COUNT(*) FROM TrackPoints) AS trackpoint_count,
            """
    
    
    cursor.execute(query % table_name)
    db_connection.commit()

    connection.close_connection()

